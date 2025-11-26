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
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.frm.Frame;
import io.questdb.cairo.frm.FrameAlgebra;
import io.questdb.cairo.frm.file.FrameFactory;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.NullMapWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCARW;
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
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.SymbolMapDiffCursor;
import io.questdb.cairo.wal.SymbolMapDiffEntry;
import io.questdb.cairo.wal.TableWriterPressureControl;
import io.questdb.cairo.wal.WalTxnDetails;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WriterRowUtils;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.ConvertOperatorImpl;
import io.questdb.griffin.DropIndexOperator;
import io.questdb.griffin.PurgingOperator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.UpdateOperatorImpl;
import io.questdb.griffin.engine.ops.AbstractOperation;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.griffin.engine.table.parquet.MappedMemoryPartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.griffin.engine.table.parquet.RowGroupStatBuffers;
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
import io.questdb.std.Decimal256;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FindVisitor;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.LongList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.Os;
import io.questdb.std.PagedDirectLongList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.Vect;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
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
import org.jetbrains.annotations.Nullable;
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
import static io.questdb.cairo.TableUtils.openAppend;
import static io.questdb.cairo.TableUtils.openRO;
import static io.questdb.cairo.sql.AsyncWriterCommand.Error.*;
import static io.questdb.std.Files.*;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
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
    public static final long TIMESTAMP_EPOCH = 0L;
    public static final int TIMESTAMP_MERGE_ENTRY_BYTES = Long.BYTES * 2;
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
    private static final int TODO_META_INDEX_OFFSET = 48;
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
    private final SOCountDownLatch indexLatch = new SOCountDownLatch();
    private final LongList indexSequences = new LongList();
    private final ObjList<ColumnIndexer> indexers;
    // This is the same message bus. When TableWriter instance is created via CairoEngine, message bus is shared
    // and is owned by the engine. Since TableWriter would not have ownership of the bus, it must not free it up.
    // On another hand, when TableWrite is created outside CairoEngine, primarily in tests, the ownership of the
    // message bus is with the TableWriter. Therefore, message bus must be freed when the writer is freed.
    // To indicate ownership, the message bus owned by the writer will be assigned to `ownMessageBus`. This reference
    // will be released by the writer
    private final MessageBus messageBus;
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
    private final DirectIntList parquetColumnIdsAndTypes;
    private final PartitionDecoder parquetDecoder = new PartitionDecoder();
    private final RowGroupStatBuffers parquetStatBuffers;
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
    private final TableWriterSegmentCopyInfo segmentCopyInfo = new TableWriterSegmentCopyInfo();
    private final TableWriterSegmentFileCache segmentFileCache;
    private final TxReader slaveTxReader;
    private final ObjList<MapWriter> symbolMapWriters;
    private final IntList symbolRewriteMap = new IntList();
    private final TimestampDriver timestampDriver;
    private final int timestampType;
    private final DirectUtf8StringZ tmpDirectUtf8StringZ = new DirectUtf8StringZ();
    private final MemoryMARW todoMem = Vm.getCMARWInstance();
    private final TxWriter txWriter;
    private final TxnScoreboard txnScoreboard;
    private final StringSink utf16Sink = new StringSink();
    private final Utf8StringSink utf8Sink = new Utf8StringSink();
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;
    private final Uuid uuid = new Uuid();
    private final LowerCaseCharSequenceIntHashMap validationMap = new LowerCaseCharSequenceIntHashMap();
    private ObjList<? extends MemoryA> activeColumns;
    private ObjList<Runnable> activeNullSetters;
    private ColumnVersionReader attachColumnVersionReader;
    private IndexBuilder attachIndexBuilder;
    private long attachMaxTimestamp;
    private MemoryCMR attachMetaMem;
    private TableWriterMetadata attachMetadata;
    private long attachMinTimestamp;
    private long attachPartitionTimestamp;
    private final FindVisitor attachPartitionPinColumnVersionsRef = this::attachPartitionPinColumnVersions;
    private TxReader attachTxReader;
    private long avgRecordSize;
    private boolean avoidIndexOnCommit = false;
    private BlockFileWriter blockFileWriter;
    private int columnCount;
    private long commitRowCount;
    private long committedMasterRef;
    private ConvertOperatorImpl convertOperatorImpl;
    private DedupColumnCommitAddresses dedupColumnCommitAddresses;
    private byte dedupMode = WalUtils.WAL_DEDUP_MODE_DEFAULT;
    private String designatedTimestampColumnName;
    private boolean distressed = false;
    private DropIndexOperator dropIndexOperator;
    private int indexCount;
    private int lastErrno;
    private boolean lastOpenPartitionIsReadOnly;
    private long lastOpenPartitionTs = Long.MIN_VALUE;
    private long lastOpenPartitionTxnName = -1;
    private long lastPartitionTimestamp;
    private long lastWalCommitTimestampMicros;
    private LifecycleManager lifecycleManager;
    private long lockFd = -2;
    private long masterRef = 0L;
    // A flag that during WAL processing o3MemColumns1 or o3MemColumns2 were
    // set to a "shifted" state and the state has to be cleaned.
    private boolean memColumnShifted;
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
    private DirectLongList tempDirectMemList;
    private long tempMem16b = Unsafe.malloc(16, MemoryTag.NATIVE_TABLE_WRITER);
    private LongConsumer timestampSetter;
    private long todoTxn;
    private final FragileCode RECOVER_FROM_SWAP_RENAME_FAILURE = this::recoverFromSwapRenameFailure;
    private final FragileCode RECOVER_FROM_COLUMN_OPEN_FAILURE = this::recoverOpenColumnFailure;
    private UpdateOperatorImpl updateOperatorImpl;
    private long walRowsProcessed;
    private WalTxnDetails walTxnDetails;
    private final ColumnTaskHandler cthMapSymbols = this::processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks_mapSymbols;
    private final ColumnTaskHandler cthMergeWalColumnManySegments = this::processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks_mergeShuffleWalColumnManySegments;

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
            CairoEngine cairoEngine
    ) {
        this(
                configuration,
                tableToken,
                messageBus,
                ownMessageBus,
                lock,
                lifecycleManager,
                root,
                ddlListener,
                checkpointStatus,
                cairoEngine,
                cairoEngine.getTxnScoreboardPool()
        );
    }

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
            CairoEngine cairoEngine,
            TxnScoreboardPool txnScoreboardPool
    ) {
        LOG.info().$("open '").$(tableToken).$('\'').$();
        this.configuration = configuration;
        this.ddlListener = ddlListener;
        this.checkpointStatus = checkpointStatus;
        this.mixedIOFlag = configuration.isWriterMixedIOEnabled();
        this.metrics = configuration.getMetrics();
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
        this.lastWalCommitTimestampMicros = configuration.getMicrosecondClock().getTicks();
        try {
            this.path = new Path();
            path.of(root);
            this.pathRootSize = configuration.getDbLogName() == null ? path.size() : 0;
            path.concat(tableToken);
            this.other = new Path();
            other.of(root).concat(tableToken);
            this.pathSize = path.size();
            if (lock) {
                lock();
            } else {
                this.lockFd = -1;
            }

            this.ddlMem = Vm.getCMARWInstance();
            // Read txn file, without accurately specifying partitionBy.
            // We need the latest txn to check if the _meta file needs to be repaired,
            // then we will read _meta file and initialize partitionBy in txWriter
            try {
                this.txWriter = new TxWriter(ff, configuration).ofRW(path.concat(TXN_FILE_NAME).$());
                path.trimTo(pathSize);
            } catch (CairoException ex) {
                // This is the first FS call the table directory.
                // If table is removed / renamed, this should fail with table does not exist.
                if (Files.isErrnoFileDoesNotExist(ex.getErrno())) {
                    throw CairoException.tableDoesNotExist(tableToken.getTableName());
                }
                throw ex;
            }
            int todo = readTodo(txWriter.txn);
            if (todo == TODO_RESTORE_META) {
                repairMetaRename(todoMem);
            }
            this.metadata = new TableWriterMetadata(this.tableToken);
            openMetaFile(ff, path, pathSize, ddlMem, metadata);
            this.timestampType = metadata.getTimestampType();
            this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
            this.partitionBy = metadata.getPartitionBy();
            this.txWriter.initPartitionBy(timestampType, metadata.getPartitionBy());

            this.txnScoreboard = txnScoreboardPool.getTxnScoreboard(tableToken);
            path.trimTo(pathSize);
            this.columnVersionWriter = openColumnVersionFile(configuration, path, pathSize, partitionBy != PartitionBy.NONE);
            this.o3ColumnOverrides = metadata.isWalEnabled() ? new ObjList<>() : null;
            this.parquetStatBuffers = new RowGroupStatBuffers(MemoryTag.NATIVE_TABLE_WRITER);
            this.parquetColumnIdsAndTypes = new DirectIntList(2, MemoryTag.NATIVE_TABLE_WRITER);

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
                this.partitionDirFmt = PartitionBy.getPartitionDirFormatMethod(timestampType, partitionBy);
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

            // wal specific
            segmentFileCache = metadata.isWalEnabled() ? new TableWriterSegmentFileCache(tableToken, configuration) : null;
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

    public static long getTimestampIndexValue(long timestampIndexAddr, long indexRow) {
        return Unsafe.getUnsafe().getLong(timestampIndexAddr + indexRow * 16);
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

    /**
     * Adds new column to table, which can be either empty or can have data already. When existing columns
     * already have data, this function will create ".top" file in addition to column files. ".top" file contains
     * the size of partition at the moment of column creation. It must be used to accurately position inside new
     * column when either appending or reading.
     *
     * <b>Failures</b>
     * Adding new column can fail in many situations. None of the failures affect the integrity of data that is already in
     * the table but can leave instance of TableWriter in inconsistent state. When this happens, function will throw CairoError.
     * Calling code must close TableWriter instance and open another when problems are rectified. Those problems would be
     * either with disk or memory or both.
     * <p>
     * Whenever function throws CairoException application code can continue using TableWriter instance and may attempt to
     * add columns again.
     *
     * <b>Transactions</b>
     * <p>
     * Pending transaction will be committed before the function attempts to add column. Even when function is unsuccessful, it may
     * still have committed transaction.
     *
     * @param columnName              of column either ASCII or UTF8 encoded.
     * @param symbolCapacity          when column columnType is a SYMBOL, this parameter specifies approximate capacity for the symbol map.
     *                                It should be equal to the number of unique symbol values stored in the table, and getting this
     *                                value badly wrong will cause performance degradation. Must be power of 2
     * @param symbolCacheFlag         when set to true, symbol values will be cached on Java heap.
     * @param columnType              {@link ColumnType}
     * @param isIndexed               configures column to be indexed or not
     * @param indexValueBlockCapacity approximation of number of rows for a single index key must be power of 2
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

        if (metadata.getColumnIndexQuiet(columnName) != -1) {
            throw CairoException.duplicateColumn(columnName);
        }

        commit();

        long columnNameTxn = getTxn();
        LOG.info()
                .$("adding column '").$safe(columnName)
                .$('[').$(ColumnType.nameOf(columnType)).$("], columnName txn ").$(columnNameTxn)
                .$(" to ").$substr(pathRootSize, path)
                .$();

        addColumnToMeta(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                isDedupKey,
                columnNameTxn,
                -1,
                metadata
        );


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
                runFragile(RECOVER_FROM_COLUMN_OPEN_FAILURE, e);
            }
        }

        clearTodoAndCommitMetaStructureVersion();

        try {
            if (!Os.isWindows()) {
                try {
                    ff.fsyncAndClose(openRO(ff, path.$(), LOG));
                } catch (CairoException e) {
                    LOG.error().$("could not fsync after column added, non-critical [path=").$(path)
                            .$(", msg=").$safe(e.getFlyweightMessage())
                            .$(", errno=").$(e.getErrno())
                            .I$();
                }
            }

            if (securityContext != null) {
                ddlListener.onColumnAdded(securityContext, tableToken, columnName);
            }

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }
        } catch (CairoError err) {
            throw err;
        } catch (Throwable th) {
            throwDistressException(th);
        }
    }

    @Override
    public void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize) {
        assert indexValueBlockSize == Numbers.ceilPow2(indexValueBlockSize) : "power of 2 expected";

        checkDistressed();

        final int columnIndex = metadata.getColumnIndexQuiet(columnName);

        if (columnIndex == -1) {
            throw CairoException.invalidMetadataRecoverable("column does not exist", columnName);
        }

        TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);

        commit();

        if (columnMetadata.isSymbolIndexFlag()) {
            throw CairoException.invalidMetadataRecoverable("column is already indexed", columnName);
        }

        final int existingType = columnMetadata.getColumnType();
        LOG.info().$("adding index to '").$safe(columnName).$("' [")
                .$(ColumnType.nameOf(existingType)).$(", path=").$substr(pathRootSize, path).I$();

        if (!ColumnType.isSymbol(existingType)) {
            LOG.error().$("cannot create index for [column='").$safe(columnName)
                    .$(", type=").$(ColumnType.nameOf(existingType))
                    .$(", path=").$substr(pathRootSize, path).I$();
            throw CairoException.invalidMetadataRecoverable("cannot create index, column type is not SYMBOL", columnName);
        }

        final SymbolColumnIndexer indexer = new SymbolColumnIndexer(configuration);
        writeIndex(columnName, indexValueBlockSize, columnIndex, indexer);

        columnMetadata.setSymbolIndexFlag(true);
        columnMetadata.setIndexValueBlockCapacity(indexValueBlockSize);

        // set the index flag in metadata and create new _meta.swp
        rewriteAndSwapMetadata(metadata);
        clearTodoAndCommitMeta();

        indexers.extendAndSet(columnIndex, indexer);
        populateDenseIndexerList();

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
        LOG.info().$("ADDED index to '").$safe(columnName).$('[').$(ColumnType.nameOf(existingType))
                .$("]' to ").$substr(pathRootSize, path).$();
    }

    public void addPhysicallyWrittenRows(long rows) {
        physicallyWrittenRowsSinceLastCommit.addAndGet(rows);
        metrics.tableWriterMetrics().addPhysicallyWrittenRows(rows);
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
            // rollback in case on any dirty state.
            // Do not catch rollback exceptions, let the calling code handle distressed writer
            rollback();

            if (ex.isWALTolerable()) {
                // Mark the transaction as applied and ignore it.
                commitSeqTxn(seqTxn);
                return 0;
            } else {
                // Mark the transaction as not applied.
                setSeqTxn(seqTxn - 1);
            }
            throw ex;
        } catch (Throwable th) {
            try {
                rollback(); // rollback seqTxn
            } catch (Throwable th2) {
                LOG.critical().$("could not rollback, table is distressed [table=")
                        .$(tableToken).$(", error=").$(th2).I$();
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
            assert !tableToken.isWal();
            LOG.info().$("committing open transaction before applying attach partition command [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, timestamp).I$();
            commit();

            // Check that the partition we're about to attach hasn't appeared after commit
            if (txWriter.attachedPartitionsContains(timestamp)) {
                LOG.info().$("partition is already attached [path=").$substr(pathRootSize, path).I$();
                return AttachDetachStatus.ATTACH_ERR_PARTITION_EXISTS;
            }
        }

        // final name of partition folder after attachment
        setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, timestamp, getTxn());
        if (ff.exists(path.$())) {
            // Very unlikely since txn is part of the folder name
            return AttachDetachStatus.ATTACH_ERR_DIR_EXISTS;
        }

        Path detachedPath = Path.PATH.get().of(configuration.getDbRoot()).concat(tableToken);
        setPathForNativePartition(detachedPath, timestampType, partitionBy, timestamp, -1L);
        detachedPath.put(configuration.getAttachPartitionSuffix()).$();
        int detachedRootLen = detachedPath.size();
        boolean forceRenamePartitionDir = partitionSize < 0;
        boolean checkPassed = false;
        boolean isSoftLink;
        boolean isParquet;
        long parquetSize = -1L;
        try {
            if (ff.exists(detachedPath.$())) {
                isSoftLink = ff.isSoftLink(detachedPath.$()); // returns false regardless in Windows

                isParquet = ff.exists(detachedPath.concat(PARQUET_PARTITION_NAME).$());
                if (isParquet) {
                    parquetSize = ff.length(detachedPath.$());
                }
                // detached metadata files validation
                CharSequence timestampColName = metadata.getColumnMetadata(metadata.getTimestampIndex()).getColumnName();
                if (partitionSize > -1L) {
                    // read detachedMinTimestamp and detachedMaxTimestamp
                    readPartitionMinMaxTimestamps(timestamp, detachedPath.trimTo(detachedRootLen), timestampColName, parquetSize, partitionSize);
                } else {
                    // read size, detachedMinTimestamp and detachedMaxTimestamp
                    partitionSize = readPartitionSizeMinMaxTimestamps(timestamp, detachedPath.trimTo(detachedRootLen), timestampColName, parquetSize);
                }

                if (partitionSize < 1) {
                    return AttachDetachStatus.ATTACH_ERR_EMPTY_PARTITION;
                }

                if (forceRenamePartitionDir && !attachPrepare(timestamp, partitionSize, detachedPath, detachedRootLen)) {
                    attachValidateMetadata(partitionSize, detachedPath.trimTo(detachedRootLen), timestamp);
                }

                // the main columnVersionWriter is now aligned with the detached partition values read from the partition _cv file
                // in case of an error it has to be clean up

                if (forceRenamePartitionDir && configuration.attachPartitionCopy() && !isSoftLink) { // soft links are read-only, no copy involved
                    // Copy partition if configured to do so, and it's not CSV import
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

                // pin column versions
                // the dir traversal will attempt to populate the column versions, we need to maintain the timestamp
                // of the attached partition
                this.attachPartitionTimestamp = timestamp;
                ff.iterateDir(path.$(), attachPartitionPinColumnVersionsRef);

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

            if (isParquet) {
                txWriter.setPartitionParquetFormat(timestamp, parquetSize, true);
            }

            txWriter.bumpTruncateVersion();

            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(denseSymbolMapWriters);

            LOG.info().$("partition attached [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, timestamp).I$();

            if (appendPartitionAttached) {
                LOG.info().$("switch partition after partition attach [tableName=").$(tableToken)
                        .$(", partition=").$ts(timestampDriver, timestamp).I$();
                freeColumns(true);
                configureAppendPosition();
            }
            return AttachDetachStatus.OK;
        } catch (Throwable e) {
            // This is pretty serious; after partition copied, there are no OS operations to fail.
            // Do full rollback to clean up the state
            LOG.critical().$("failed on attaching partition to the table and rolling back [tableName=").$(tableToken)
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
            TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
            columnMetadata.setSymbolCacheFlag(cache);
            writeMetadataToDisk();
        }
    }

    @Override
    public void changeColumnType(
            CharSequence name,
            int newType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            SecurityContext securityContext
    ) {
        int existingColIndex = metadata.getColumnIndexQuiet(name);
        if (existingColIndex < 0) {
            throw CairoException.nonCritical().put("cannot change column type, column does not exist [table=")
                    .put(tableToken.getTableName()).put(", column=").put(name).put(']');
        }
        String columnName = metadata.getColumnName(existingColIndex);

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

            LOG.info().$("converting column [table=").$(tableToken).$(", column=").$safe(columnName)
                    .$(", from=").$(ColumnType.nameOf(existingType))
                    .$(", to=").$(ColumnType.nameOf(newType)).I$();

            boolean isDedupKey = metadata.isDedupKey(existingColIndex);
            int columnIndex = columnCount;
            long columnNameTxn = getTxn();

            // Set txn number in the column version file to mark the transaction where the column is added
            long firstPartitionTsm = columnVersionWriter.getColumnTopPartitionTimestamp(existingColIndex);
            columnVersionWriter.upsertDefaultTxnName(columnIndex, columnNameTxn, firstPartitionTsm);

            if (ColumnType.isSymbol(newType)) {
                createSymbolMapWriter(columnName, columnNameTxn, symbolCapacity, symbolCacheFlag, columnIndex);
            } else {
                // maintain a sparse list of symbol writers
                symbolMapWriters.extendAndSet(columnCount, NullMapWriter.INSTANCE);
            }
            boolean existingIsIndexed = metadata.isColumnIndexed(existingColIndex) && existingType == ColumnType.SYMBOL;
            convertOperator.convertColumn(columnName, existingColIndex, existingType, existingIsIndexed, columnIndex, newType);

            // Column converted, add new one to _meta file and remove the existing column
            metadata.removeColumn(existingColIndex);

            // close old column files
            freeColumnMemory(existingColIndex);

            // remove symbol map writer or entry for such
            removeSymbolMapWriter(existingColIndex);

            addColumnToMeta(
                    columnName,
                    newType,
                    symbolCapacity,
                    symbolCacheFlag,
                    isIndexed,
                    indexValueBlockCapacity,
                    isDedupKey,
                    columnNameTxn,
                    existingColIndex,
                    metadata
            );

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
            // it uses the primary column object as a temporary tool
            if (isIndexed) {
                SymbolColumnIndexer indexer = (SymbolColumnIndexer) indexers.get(columnIndex);
                writeIndex(columnName, indexValueBlockCapacity, columnIndex, indexer);
                // add / remove indexers
                indexers.extendAndSet(columnIndex, indexer);
                populateDenseIndexerList();
            }

            clearTodoAndCommitMetaStructureVersion();
        } catch (Throwable th) {
            LOG.critical().$("could not change column type [table=").$(tableToken).$(", column=").$safe(columnName)
                    .$(", error=").$(th).I$();
            distressed = true;
            throw th;
        } finally {
            // clear temp resources
            convertOperator.finishColumnConversion();
            path.trimTo(pathSize);
        }

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    @Override
    public void changeSymbolCapacity(
            CharSequence colName,
            int newSymbolCapacity,
            SecurityContext securityContext
    ) {
        int columnIndex = metadata.getColumnIndexQuiet(colName);
        if (columnIndex < 0) {
            // Log it as non-critical because it's not a structural change.
            // It is possible in concurrent schema modification that SQL compiler allowed
            // this alter, but by the time it is applied, the colum type has changed.
            LOG.error().$("cannot change column type, column does not exist [table=").$(tableToken)
                    .$(", column=").$safe(colName).I$();
            return;
        }

        String columnName = metadata.getColumnName(columnIndex);
        int existingType = metadata.getColumnType(columnIndex);
        assert existingType > 0;

        if (!ColumnType.isSymbol(existingType)) {
            // Log it as non-critical because it's not a structural change.
            // It is possible in concurrent schema modification that SQL compiler allowed
            // this alter, but by the time it is applied, the colum type has changed.
            LOG.error().$("cannot symbol capacity, column is not symbol [table=").$(tableToken)
                    .$(", column=").$safe(columnName).$(", columnType=").$(ColumnType.nameOf(existingType)).I$();
            return;
        }


        var oldSymbolWriter = (SymbolMapWriter) symbolMapWriters.getQuick(columnIndex);
        int oldCapacity = oldSymbolWriter.getSymbolCapacity();

        newSymbolCapacity = Numbers.ceilPow2(newSymbolCapacity);

        if (oldCapacity == newSymbolCapacity) {
            // Nothing to do.
            return;
        }
        try {
            TableUtils.validateSymbolCapacity(0, newSymbolCapacity);
        } catch (SqlException e) {
            LOG.error().$("invalid symbol capacity to change to [table=").$(tableToken)
                    .$(", column=").$safe(columnName)
                    .$(", from=").$(oldCapacity)
                    .$(", to=").$(newSymbolCapacity)
                    .I$();

            throw CairoException.nonCritical().put("invalid symbol capacity [name=").put(columnName).put(", capacity=").put(newSymbolCapacity).put(']');
        }

        LOG.info().$("changing symbol capacity [table=").$(tableToken).$(", column=").$safe(columnName)
                .$(", from=").$(oldCapacity)
                .$(", to=").$(newSymbolCapacity).I$();


        try {
            boolean symbolCacheFlag = metadata.getColumnMetadata(columnIndex).isSymbolCacheFlag();
            long columnNameTxn = getTxn();
            metadata.updateColumnSymbolCapacity(columnIndex, newSymbolCapacity);

            try {
                rewriteAndSwapMetadata(metadata);
                hardLinkAndPurgeSymbolTableFiles(
                        columnName,
                        columnIndex,
                        metadata.isIndexed(columnIndex),
                        columnName
                );
                oldSymbolWriter.rebuildCapacity(
                        configuration,
                        path,
                        columnName,
                        columnNameTxn,
                        newSymbolCapacity,
                        symbolCacheFlag
                );

                bumpMetadataVersion();
            } catch (CairoException e) {
                throwDistressException(e);
            }

            // remove _todo as last step, after the commit.
            // if anything fails before the commit, meta file will be reverted
            clearTodoLog();

            try {

                // Call finish purge to remove old column files before renaming them in metadata
                finishColumnPurge();

                // open new column files
                long transientRowCount = txWriter.getTransientRowCount();
                if (transientRowCount > 0) {
                    long partitionTimestamp = txWriter.getLastPartitionTimestamp();
                    setStateForTimestamp(path, partitionTimestamp);
                    int plen = path.size();
                    // column name txn for partition columns would not change
                    // we updated only symbol table version
                    columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                    openColumnFiles(columnName, columnNameTxn, columnIndex, path.size());
                    setColumnAppendPosition(columnIndex, transientRowCount, false);
                    path.trimTo(pathSize);

                    if (metadata.isIndexed(columnIndex)) {
                        ColumnIndexer indexer = indexers.get(columnIndex);
                        final long columnTop = columnVersionWriter.getColumnTopQuick(partitionTimestamp, columnIndex);
                        assert indexer != null;
                        indexer.configureFollowerAndWriter(path.trimTo(plen), columnName, columnNameTxn, getPrimaryColumn(columnIndex), columnTop);
                    }
                }
            } catch (Throwable th) {
                handleHousekeepingException(th);
            }
        } finally {
            partitionRemoveCandidates.clear();
            // clear temp resources
            path.trimTo(pathSize);
        }

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    public boolean checkScoreboardHasReadersBeforeLastCommittedTxn() {
        if (checkpointStatus.partitionsLocked()) {
            // do not alter scoreboard while checkpoint is in progress
            return true;
        }
        return txnScoreboard.hasEarlierTxnLocks(txWriter.getTxn());
    }

    @Override
    public void close() {
        if (lifecycleManager.close() && isOpen()) {
            doClose(true);
        }
    }

    public void closeActivePartition(boolean truncate) {
        LOG.debug().$("closing last partition [table=").$(tableToken).I$();
        closeAppendMemoryTruncate(truncate);
        freeIndexers();
    }

    public ColumnVersionReader columnVersionReader() {
        return columnVersionWriter;
    }

    @Override
    public void commit() {
        commit(0);
    }

    public void commitSeqTxn(long seqTxn) {
        txWriter.setSeqTxn(seqTxn);
        txWriter.commit(denseSymbolMapWriters);
    }

    public void commitSeqTxn() {
        if (txWriter.inTransaction()) {
            metrics.tableWriterMetrics().incrementCommits();
            syncColumns();
        }
        txWriter.commit(denseSymbolMapWriters);
    }

    public void commitWalInsertTransactions(
            @Transient Path walPath,
            long seqTxn,
            TableWriterPressureControl pressureControl
    ) {
        if (hasO3() || columnVersionWriter.hasChanges()) {
            // When the writer is returned to the pool, it should be rolled back. Having an open transaction is very suspicious.
            // Set the writer to distressed state and throw exception so that the writer is re-created.
            distressed = true;
            throw CairoException.critical(0).put("cannot process WAL while in transaction");
        }

        physicallyWrittenRowsSinceLastCommit.set(0);
        txWriter.beginPartitionSizeUpdate();
        long commitToTimestamp = walTxnDetails.getCommitToTimestamp(seqTxn);
        int transactionBlock = calculateInsertTransactionBlock(seqTxn, pressureControl);

        boolean committed;
        final long initialCommittedRowCount = txWriter.getRowCount();
        walRowsProcessed = 0;

        try {
            if (transactionBlock == 1) {
                committed = processWalCommit(walPath, seqTxn, pressureControl, commitToTimestamp);
            } else {
                try {
                    int blockSize = processWalCommitBlock(
                            seqTxn,
                            transactionBlock,
                            pressureControl
                    );
                    committed = blockSize > 0;
                    seqTxn += blockSize - 1;
                } catch (CairoException e) {
                    if (e.isBlockApplyError()) {
                        if (configuration.getDebugWalApplyBlockFailureNoRetry()) {
                            // Do not re-try the application as 1 by 1 in tests.
                            throw e;
                        }
                        pressureControl.onBlockApplyError();
                        pressureControl.updateInflightTxnBlockLength(
                                1,
                                Math.max(1, walTxnDetails.getSegmentRowHi(seqTxn) - walTxnDetails.getSegmentRowLo(seqTxn))
                        );
                        LOG.info().$("failed to apply block, trying to apply 1 by 1 [table=").$(tableToken)
                                .$(", startTxn=").$(seqTxn)
                                .I$();
                        // Try applying 1 transaction at a time
                        committed = processWalCommit(walPath, seqTxn, pressureControl, commitToTimestamp);
                    } else {
                        throw e;
                    }
                }
            }
        } catch (CairoException e) {
            if (e.isOutOfMemory()) {
                // oom -> we cannot rely on internal TableWriter consistency, all bets are off, better to discard it and re-recreate
                distressed = true;
            }
            throw e;
        }

        walTxnDetails.setIncrementRowsCommitted(walRowsProcessed);
        if (committed) {
            assert txWriter.getLagRowCount() == 0;

            txWriter.setSeqTxn(seqTxn);
            txWriter.setLagTxnCount(0);
            txWriter.setLagOrdered(true);

            commit00();
            lastWalCommitTimestampMicros = configuration.getMicrosecondClock().getTicks();
            housekeep();
            shrinkO3Mem();

            assert txWriter.getPartitionCount() == 0 || txWriter.getMinTimestamp() >= txWriter.getPartitionTimestampByIndex(0);
            LOG.debug().$("table ranges after the commit [table=").$(tableToken)
                    .$(", minTs=").$ts(timestampDriver, txWriter.getMinTimestamp())
                    .$(", maxTs=").$ts(timestampDriver, txWriter.getMaxTimestamp()).I$();
        }

        // Sometimes nothing is committed to the table, only copied to LAG.
        // Sometimes data from LAG is made visible to the table using fast commit that increment transient row count.
        // Keep in memory last committed seq txn, but do not write it to _txn file.
        assert txWriter.getLagTxnCount() == (seqTxn - txWriter.getSeqTxn());
        metrics.tableWriterMetrics().addCommittedRows(txWriter.getRowCount() - initialCommittedRowCount);
    }

    @Override
    public boolean convertPartitionNativeToParquet(long partitionTimestamp) {
        final int memoryTag = MemoryTag.MMAP_PARQUET_PARTITION_CONVERTER;

        assert metadata.getTimestampIndex() > -1;
        assert PartitionBy.isPartitioned(partitionBy);

        if (inTransaction()) {
            assert !tableToken.isWal();
            LOG.info()
                    .$("committing open transaction before applying convert partition to parquet command [table=")
                    .$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .I$();
            commit();
        }

        partitionTimestamp = txWriter.getLogicalPartitionTimestamp(partitionTimestamp);

        if (partitionTimestamp == txWriter.getLogicalPartitionTimestamp(txWriter.getMaxTimestamp())) {
            // The partition is active; conversion is currently unsupported.
            LOG.info()
                    .$("skipping active partition as it cannot be converted to parquet format [table=")
                    .$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .I$();
            return true;
        }

        final int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
        if (partitionIndex < 0) {
            formatPartitionForTimestamp(partitionTimestamp, -1);
            throw CairoException.nonCritical().put("cannot convert partition to parquet, partition does not exist [table=").put(tableToken.getTableName())
                    .put(", partition=").put(utf8Sink).put(']');
        }

        if (txWriter.isPartitionParquet(partitionIndex)) {
            return true; // Partition is already in Parquet format.
        }
        lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
        boolean lastPartitionConverted = lastPartitionTimestamp == partitionTimestamp;
        squashPartitionForce(partitionIndex);

        long partitionNameTxn = txWriter.getPartitionNameTxn(partitionIndex);

        setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        final int partitionDirLen = path.size();
        if (!ff.exists(path.$())) {
            throw CairoException.nonCritical().put("partition directory does not exist [path=").put(path).put(']');
        }

        // upgrade partition version
        setPathForNativePartition(other.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, getTxn());
        createDirsOrFail(ff, other, configuration.getMkDirMode());
        final int newPartitionDirLen = other.size();

        // set the parquet file full path
        setPathForParquetPartition(other.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, getTxn());

        LOG.info().$("converting partition to parquet [path=").$substr(pathRootSize, path).I$();
        long parquetFileLength;
        try {
            try (PartitionDescriptor partitionDescriptor = new MappedMemoryPartitionDescriptor(ff)) {
                final long partitionRowCount = getPartitionSize(partitionIndex);
                final int timestampIndex = metadata.getTimestampIndex();
                partitionDescriptor.of(getTableToken().getTableName(), partitionRowCount, timestampIndex);

                final int columnCount = metadata.getColumnCount();
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    final int columnType = metadata.getColumnType(columnIndex);
                    if (columnType <= 0) {
                        continue; // skip deleted columns
                    }
                    final String columnName = metadata.getColumnName(columnIndex);
                    final int columnId = metadata.getColumnMetadata(columnIndex).getWriterIndex();

                    final long columnNameTxn = getColumnNameTxn(partitionTimestamp, columnIndex);
                    final long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);
                    final long columnRowCount = (columnTop != -1) ? partitionRowCount - columnTop : 0;

                    if (columnRowCount > 0) {
                        if (ColumnType.isSymbol(columnType)) {
                            partitionDescriptor.addColumn(
                                    columnName,
                                    columnType,
                                    columnId,
                                    columnTop
                            );

                            final long columnSize = columnRowCount * ColumnType.sizeOf(columnType);
                            final long columnAddr = mapRO(ff, dFile(path.trimTo(partitionDirLen), columnName, columnNameTxn), LOG, columnSize, memoryTag);
                            partitionDescriptor.setColumnAddr(columnAddr, columnSize);

                            // root symbol files use separate txn
                            final long symbolTableNameTxn = columnVersionWriter.getSymbolTableNameTxn(columnIndex);

                            offsetFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn);
                            if (!ff.exists(path.$())) {
                                LOG.error().$(path).$(" is not found").$();
                                throw CairoException.fileNotFound().put("offset file does not exist: ").put(path);
                            }

                            final long fileLength = ff.length(path.$());
                            if (fileLength < SymbolMapWriter.HEADER_SIZE) {
                                LOG.error().$(path).$("symbol file is too small [fileLength=").$(fileLength).$(']').$();
                                throw CairoException.critical(0).put("SymbolMap is too short: ").put(path);
                            }

                            final int symbolCount = getSymbolMapWriter(columnIndex).getSymbolCount();
                            final long offsetsMemSize = SymbolMapWriter.keyToOffset(symbolCount + 1);
                            final long symbolOffsetsAddr = mapRO(ff, path.$(), LOG, offsetsMemSize, memoryTag);
                            partitionDescriptor.setSymbolOffsetsAddr(symbolOffsetsAddr + HEADER_SIZE, symbolCount);

                            final LPSZ charFileName = charFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn);
                            final long columnSecondarySize = ff.length(charFileName);
                            final long columnSecondaryAddr = mapRO(ff, charFileName, LOG, columnSecondarySize, memoryTag);
                            partitionDescriptor.setSecondaryColumnAddr(columnSecondaryAddr, columnSecondarySize);

                            // recover the partition path
                            setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
                        } else if (ColumnType.isVarSize(columnType)) {
                            partitionDescriptor.addColumn(columnName, columnType, columnId, columnTop);

                            final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                            final long auxVectorSize = columnTypeDriver.getAuxVectorSize(columnRowCount);
                            final long auxVectorAddr = mapRO(ff, iFile(path.trimTo(partitionDirLen), columnName, columnNameTxn), LOG, auxVectorSize, memoryTag);
                            partitionDescriptor.setSecondaryColumnAddr(auxVectorAddr, auxVectorSize);

                            final long dataSize = columnTypeDriver.getDataVectorSizeAt(auxVectorAddr, columnRowCount - 1);
                            if (dataSize < columnTypeDriver.getDataVectorMinEntrySize() || dataSize >= (1L << 40)) {
                                LOG.critical().$("Invalid var len column size [column=").$safe(columnName)
                                        .$(", size=").$(dataSize)
                                        .$(", path=").$(path)
                                        .I$();
                                throw CairoException.critical(0).put("Invalid column size [column=").put(path)
                                        .put(", size=").put(dataSize)
                                        .put(']');
                            }

                            final long dataAddr = dataSize > 0
                                    ? mapRO(ff, dFile(path.trimTo(partitionDirLen), columnName, columnNameTxn), LOG, dataSize, memoryTag)
                                    : 0;
                            partitionDescriptor.setColumnAddr(dataAddr, dataSize);
                        } else {
                            final long mapBytes = columnRowCount * ColumnType.sizeOf(columnType);
                            final long fixedAddr = mapRO(ff, dFile(path.trimTo(partitionDirLen), columnName, columnNameTxn), LOG, mapBytes, memoryTag);
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
                    } else {
                        // no rows in column
                        partitionDescriptor.addColumn(
                                columnName,
                                columnType,
                                columnId,
                                partitionRowCount
                        );
                    }
                }

                final CairoConfiguration config = this.getConfiguration();
                final int compressionCodec = config.getPartitionEncoderParquetCompressionCodec();
                final int compressionLevel = config.getPartitionEncoderParquetCompressionLevel();
                final int rowGroupSize = config.getPartitionEncoderParquetRowGroupSize();
                final int dataPageSize = config.getPartitionEncoderParquetDataPageSize();
                final boolean statisticsEnabled = config.isPartitionEncoderParquetStatisticsEnabled();
                final boolean rawArrayEncoding = config.isPartitionEncoderParquetRawArrayEncoding();
                final int parquetVersion = config.getPartitionEncoderParquetVersion();

                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        other,
                        ParquetCompression.packCompressionCodecLevel(compressionCodec, compressionLevel),
                        statisticsEnabled,
                        rawArrayEncoding,
                        rowGroupSize,
                        dataPageSize,
                        parquetVersion
                );
                parquetFileLength = ff.length(other.$());
            }
        } catch (CairoException e) {
            LOG.error().$("could not convert partition to parquet [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .$(", error=").$safe(e.getMessage()).I$();

            // rollback
            if (!ff.rmdir(other.trimTo(newPartitionDirLen).slash())) {
                LOG.error().$("could not remove parquet file [path=").$(other).I$();
            }
            throw e;
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }

        LOG.info().$("copying index files to parquet [path=").$substr(pathRootSize, path).I$();
        copyPartitionIndexFiles(partitionTimestamp, partitionDirLen, newPartitionDirLen);

        final long originalSize = txWriter.getPartitionSize(partitionIndex);
        // used to update txn and bump recordStructureVersion
        txWriter.updatePartitionSizeAndTxnByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION, originalSize);
        txWriter.setPartitionParquetFormat(partitionTimestamp, parquetFileLength);
        txWriter.bumpPartitionTableVersion();
        txWriter.commit(denseSymbolMapWriters);

        if (lastPartitionConverted) {
            closeActivePartition(false);
        }

        // remove old partition dir
        safeDeletePartitionDir(partitionTimestamp, partitionNameTxn);

        if (lastPartitionConverted) {
            // Open last partition as read-only
            openPartition(partitionTimestamp, txWriter.getTransientRowCount());
            setAppendPosition(txWriter.getTransientRowCount(), false);
        }
        return true;
    }

    @Override
    public boolean convertPartitionParquetToNative(long partitionTimestamp) {
        assert metadata.getTimestampIndex() > -1;
        assert PartitionBy.isPartitioned(partitionBy);

        if (inTransaction()) {
            LOG.info()
                    .$("committing open transaction before applying convert partition to parquet command [table=")
                    .$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .I$();
            commit();
        }

        partitionTimestamp = txWriter.getLogicalPartitionTimestamp(partitionTimestamp);
        final int partitionIndex = txWriter.getPartitionIndex(partitionTimestamp);
        if (partitionIndex < 0) {
            formatPartitionForTimestamp(partitionTimestamp, -1);
            throw CairoException.nonCritical().put("cannot convert parquet partition to native, partition does not exist [table=").put(tableToken.getTableName())
                    .put(", partition=").put(utf8Sink)
                    .put(']');
        }

        if (!txWriter.isPartitionParquet(partitionIndex)) {
            return true; // Partition already has a Native format
        }

        lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
        boolean lastPartitionConverted = lastPartitionTimestamp == partitionTimestamp;

        long partitionNameTxn = txWriter.getPartitionNameTxn(partitionIndex);
        setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        final int partitionDirLen = path.size();
        // set the parquet file full path
        setPathForParquetPartition(path.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        if (!ff.exists(path.$())) {
            throw CairoException.nonCritical().put("partition path does not exist [path=").put(path).put(']');
        }

        // upgrade partition version
        setPathForNativePartition(other.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, getTxn());
        createDirsOrFail(ff, other, configuration.getMkDirMode());
        final int newPartitionDirLen = other.size();

        // packed as [auxFd, dataFd, dataVecBytesWritten]
        // dataVecBytesWritten is used to adjust offsets in the auxiliary vector
        final DirectLongList columnFdAndDataSize = getTempDirectLongList(3L * columnCount);

        // path is now pointing to the parquet file
        // other is pointing to the new partition folder
        LOG.info().$("converting parquet partition to native [path=").$substr(pathRootSize, path).I$();
        final long parquetSize = txWriter.getPartitionParquetFileSize(partitionIndex);
        final long parquetAddr = mapRO(ff, path.$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);

        long parquetRowCount = 0;
        try (RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER)) {
            parquetDecoder.of(parquetAddr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
            final GenericRecordMetadata metadata = new GenericRecordMetadata();
            final PartitionDecoder.Metadata parquetMetadata = parquetDecoder.metadata();
            parquetMetadata.copyToSansUnsupported(metadata, false);

            parquetColumnIdsAndTypes.clear();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final int columnType = metadata.getColumnType(i);
                final String columnName = metadata.getColumnName(i);
                final long columnNameTxn = getColumnNameTxn(partitionTimestamp, i);

                parquetColumnIdsAndTypes.add(i);
                parquetColumnIdsAndTypes.add(columnType);

                if (ColumnType.isVarSize(columnType)) {
                    final long auxIndex = columnFdAndDataSize.size();
                    columnFdAndDataSize.add(-1); // aux
                    columnFdAndDataSize.add(-1); // data
                    columnFdAndDataSize.add(0);  // data bytes written
                    final long dstAuxFd = openAppend(ff, iFile(other.trimTo(newPartitionDirLen), columnName, columnNameTxn), LOG);
                    columnFdAndDataSize.set(auxIndex, dstAuxFd);
                    final long dstDataFd = openAppend(ff, dFile(other.trimTo(newPartitionDirLen), columnName, columnNameTxn), LOG);
                    columnFdAndDataSize.set(auxIndex + 1, dstDataFd);
                } else {
                    final long auxIndex = columnFdAndDataSize.size();
                    columnFdAndDataSize.add(-1); // aux
                    columnFdAndDataSize.add(-1); // data
                    columnFdAndDataSize.add(0);  // data bytes written
                    final long dstFixFd = openAppend(ff, dFile(other.trimTo(newPartitionDirLen), columnName, columnNameTxn), LOG);
                    columnFdAndDataSize.set(auxIndex + 1, dstFixFd);
                }
            }

            final int rowGroupCount = parquetMetadata.getRowGroupCount();
            for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                final long rowGroupRowCount = parquetDecoder.decodeRowGroup(
                        rowGroupBuffers,
                        parquetColumnIdsAndTypes,
                        rowGroupIndex,
                        0,
                        parquetMetadata.getRowGroupSize(rowGroupIndex)
                );
                parquetRowCount += rowGroupRowCount;
                for (int columnIndex = 0, n = metadata.getColumnCount(); columnIndex < n; columnIndex++) {
                    final int columnType = metadata.getColumnType(columnIndex);

                    final long srcDataPtr = rowGroupBuffers.getChunkDataPtr(columnIndex);
                    final long srcDataSize = rowGroupBuffers.getChunkDataSize(columnIndex);

                    long srcAuxPtr = rowGroupBuffers.getChunkAuxPtr(columnIndex);
                    long srcAuxSize = rowGroupBuffers.getChunkAuxSize(columnIndex);

                    if (ColumnType.isVarSize(columnType)) {
                        ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                        final long dstAuxFd = columnFdAndDataSize.get(3L * columnIndex);
                        final long dstDataFd = columnFdAndDataSize.get(3L * columnIndex + 1);
                        final long dataVecBytesWritten = columnFdAndDataSize.get(3L * columnIndex + 2);

                        if (rowGroupIndex > 0) {
                            // Adjust offsets in the auxiliary vector
                            columnTypeDriver.shiftCopyAuxVector(-dataVecBytesWritten, srcAuxPtr, 0, rowGroupRowCount - 1, srcAuxPtr, srcAuxSize);
                            // Remove the extra entry for string columns
                            final long adjust = columnTypeDriver.getMinAuxVectorSize();
                            srcAuxPtr += adjust;
                            srcAuxSize -= adjust;
                        }

                        appendBuffer(dstDataFd, srcDataPtr, srcDataSize);
                        appendBuffer(dstAuxFd, srcAuxPtr, srcAuxSize);
                        columnFdAndDataSize.set(3L * columnIndex + 2, dataVecBytesWritten + srcDataSize);
                    } else {
                        final long dstFixFd = columnFdAndDataSize.get(3L * columnIndex + 1);
                        appendBuffer(dstFixFd, srcDataPtr, srcDataSize);
                    }
                }
            }
        } catch (CairoException e) {
            LOG.error().$("could not convert partition to native [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .$(", error=").$safe(e.getMessage()).I$();

            // rollback
            if (!ff.rmdir(other.trimTo(newPartitionDirLen).slash())) {
                LOG.error().$("could not remove native partition dir [path=").$(other).I$();
            }
            throw e;
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
            for (long i = 0, n = columnFdAndDataSize.size() / 3; i < n; i++) {
                final long dstAuxFd = columnFdAndDataSize.get(3L * i);
                ff.close(dstAuxFd);
                final long dstDataFd = columnFdAndDataSize.get(3L * i + 1);
                ff.close(dstDataFd);
            }
            columnFdAndDataSize.resetCapacity();
            parquetDecoder.close();
            ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
        }

        LOG.info().$("copying index files to native [path=").$substr(pathRootSize, path).I$();
        copyPartitionIndexFiles(partitionTimestamp, partitionDirLen, newPartitionDirLen);

        // used to update txn and bump recordStructureVersion
        txWriter.updatePartitionSizeAndTxnByRawIndex(partitionIndex * LONGS_PER_TX_ATTACHED_PARTITION, parquetRowCount);
        txWriter.resetPartitionParquetFormat(partitionTimestamp);
        txWriter.bumpPartitionTableVersion();
        txWriter.commit(denseSymbolMapWriters);

        if (lastPartitionConverted) {
            closeActivePartition(false);
        }

        // remove old partition dir
        safeDeletePartitionDir(partitionTimestamp, partitionNameTxn);

        if (lastPartitionConverted) {
            // Open last partition as read-only
            openPartition(partitionTimestamp, txWriter.getTransientRowCount());
            setAppendPosition(txWriter.getTransientRowCount(), false);
        }
        return true;
    }

    public void destroy() {
        // Closes all the files and makes this instance unusable e.g., it cannot return to the pool on close.
        LOG.info().$("closing table files [table=").$(tableToken).I$();
        distressed = true;
        doClose(false);
    }

    public AttachDetachStatus detachPartition(long timestamp) {
        // Should be checked by SQL compiler
        assert metadata.getTimestampIndex() > -1;
        assert PartitionBy.isPartitioned(partitionBy);

        if (inTransaction()) {
            assert !tableToken.isWal();
            LOG.info()
                    .$("committing open transaction before applying detach partition command [table=")
                    .$(tableToken)
                    .$(", partition=").$ts(timestampDriver, timestamp)
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

        // To detach the partition, squash it into a single folder if required
        squashPartitionForce(partitionIndex);

        // To check that partition is squashed, get the next partition and
        // verify that it's not the same timestamp as the one we are trying to detach.
        // The next partition should exist, since the last partition cannot be detached.
        assert txWriter.getLogicalPartitionTimestamp(txWriter.getPartitionTimestampByIndex(partitionIndex + 1)) != timestamp;

        long minTimestamp = txWriter.getMinTimestamp();
        long partitionNameTxn = txWriter.getPartitionNameTxn(partitionIndex);
        Path detachedPath = Path.PATH.get();

        try {
            // path: partition folder to be detached
            setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, timestamp, partitionNameTxn);
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
                detachedPath.of(configuration.getDbRoot()).concat(tableToken.getDirName());
                int detachedRootLen = detachedPath.size();
                // detachedPath: detached partition folder
                if (!ff.exists(detachedPath.slash$())) {
                    // the detached and standard folders can have different roots
                    // (server.conf: cairo.sql.detached.root)
                    if (0 != ff.mkdirs(detachedPath, detachedMkDirMode)) {
                        LOG.error().$("could no create detached partition folder [errno=").$(ff.errno())
                                .$(", path=").$(detachedPath)
                                .I$();
                        return AttachDetachStatus.DETACH_ERR_MKDIR;
                    }
                }
                setPathForNativePartition(detachedPath.trimTo(detachedRootLen), timestampType, partitionBy, timestamp, -1L);
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
                    nextMinTimestamp = readMinTimestamp();
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
        LOG.info().$("disabling row deduplication [table=").$(tableToken).I$();
        for (int i = 0; i < columnCount; i++) {
            metadata.getColumnMetadata(i).setDedupKeyFlag(false);
        }
        rewriteAndSwapMetadata(metadata);
        clearTodoAndCommitMetaStructureVersion();
        if (dedupColumnCommitAddresses != null) {
            dedupColumnCommitAddresses.setDedupColumnCount(0);
        }

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    @Override
    public void dropIndex(@NotNull CharSequence name) {
        checkDistressed();

        final int columnIndex = metadata.getColumnIndexQuiet(name);
        if (columnIndex == -1) {
            throw CairoException.invalidMetadataRecoverable("column does not exist", name);
        }
        String columnName = metadata.getColumnName(columnIndex);

        TableColumnMetadata columnMetadata = metadata.getColumnMetadata(columnIndex);
        if (!columnMetadata.isSymbolIndexFlag()) {
            // if a column is indexed, it is also of type SYMBOL
            throw CairoException.invalidMetadataRecoverable("column is not indexed", columnName);
        }
        final int defaultIndexValueBlockSize = Numbers.ceilPow2(configuration.getIndexValueBlockSize());

        if (inTransaction()) {
            assert !tableToken.isWal();
            LOG.info()
                    .$("committing current transaction before DROP INDEX execution [txn=").$(txWriter.getTxn())
                    .$(", table=").$(tableToken)
                    .$(", column=").$safe(columnName)
                    .I$();
            commit();
        }

        try {
            LOG.info().$("removing index [txn=").$(txWriter.getTxn())
                    .$(", table=").$(tableToken)
                    .$(", column=").$safe(columnName)
                    .I$();
            // drop index
            if (dropIndexOperator == null) {
                dropIndexOperator = new DropIndexOperator(configuration, this, path, other, pathSize, getPurgingOperator());
            }
            dropIndexOperator.executeDropIndex(columnName, columnIndex); // upserts column version in partitions
            // swap meta commit

            // refresh metadata
            columnMetadata.setSymbolIndexFlag(false);
            columnMetadata.setIndexValueBlockCapacity(defaultIndexValueBlockSize);
            rewriteAndSwapMetadata(metadata);
            clearTodoAndCommitMeta();

            // remove indexer
            ColumnIndexer columnIndexer = indexers.getQuick(columnIndex);
            if (columnIndexer != null) {
                indexers.setQuick(columnIndex, null);
                Misc.free(columnIndexer);
                populateDenseIndexerList();
            }

            // purge old column versions
            finishColumnPurge();
            LOG.info().$("REMOVED index [txn=").$(txWriter.getTxn()).I$();

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }

            LOG.info().$("END DROP INDEX [txn=").$(txWriter.getTxn())
                    .$(", table=").$(tableToken)
                    .$(", column=").$safe(columnName)
                    .I$();
        } catch (CairoException e) {
            // LOG original exception detail
            LOG.critical().$("exception on index drop [txn=").$(txWriter.getTxn())
                    .$(", table=").$(tableToken)
                    .$(", column=").$safe(columnName)
                    .$(", errno=").$(e.errno)
                    .$(", error=").$((Throwable) e).I$();

            throw CairoException.critical(e.errno)
                    .put("cannot remove index for [txn=").put(txWriter.getTxn())
                    .put(", table=").put(tableToken.getTableName())
                    .put(", column=").put(columnName)
                    .put("]: ").put(e.getMessage());
        } catch (Throwable e) {
            // LOG original exception detail
            LOG.critical().$("exception on index drop [txn=").$(txWriter.getTxn())
                    .$(", table=").$(tableToken)
                    .$(", column=").$safe(columnName)
                    .$(", error=").$(e).I$();

            throw CairoException.critical(0)
                    .put("cannot remove index for [txn=").put(txWriter.getTxn())
                    .put(", table=").put(tableToken.getTableName())
                    .put(", column=").put(columnName)
                    .put("]: ").put(e.getMessage());
        }
    }

    @Override
    public boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
        assert txWriter.getLagRowCount() == 0;
        checkDistressed();
        LogRecord logRec = LOG.info().$("enabling row deduplication [table=").$(tableToken).$(", columns=[");

        boolean isSubsetOfOldKeys = true;
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

                isSubsetOfOldKeys &= metadata.isDedupKey(dedupColIndex);

                if (i > 0) {
                    logRec.$(',');
                }
                logRec.$safe(getColumnNameSafe(dedupColIndex)).$(':').$(ColumnType.nameOf(columnType));
            }
        } finally {
            logRec.I$();
        }

        columnsIndexes.sort();
        for (int i = 0, j = 0; i < columnCount && j < columnsIndexes.size(); i++) {
            if (i == columnsIndexes.getQuick(j)) {
                // Set dedup key flag for the column
                metadata.getColumnMetadata(i).setDedupKeyFlag(true);
                // Go to the next dedup column index
                j++;
            }
        }
        rewriteAndSwapMetadata(metadata);
        clearTodoAndCommitMetaStructureVersion();

        if (dedupColumnCommitAddresses == null) {
            dedupColumnCommitAddresses = new DedupColumnCommitAddresses();
        } else {
            dedupColumnCommitAddresses.clear();
        }
        dedupColumnCommitAddresses.setDedupColumnCount(columnsIndexes.size() - 1);

        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
        return isSubsetOfOldKeys;
    }

    public void enforceTtl() {
        partitionRemoveCandidates.clear();
        final int ttl = metadata.getTtlHoursOrMonths();
        if (ttl == 0) {
            return;
        }
        if (metadata.getPartitionBy() == PartitionBy.NONE) {
            LOG.error().$("TTL set on a non-partitioned table. Ignoring");
            return;
        }
        if (getPartitionCount() < 2) {
            return;
        }
        long maxTimestamp = getMaxTimestamp();
        long evictedPartitionTimestamp = -1;
        boolean dropped = false;
        do {
            long partitionTimestamp = getPartitionTimestamp(0);
            long floorTimestamp = txWriter.getPartitionFloor(partitionTimestamp);
            if (evictedPartitionTimestamp != -1 && floorTimestamp == evictedPartitionTimestamp) {
                assert partitionTimestamp != floorTimestamp : "Expected a higher part of a split partition";
                dropped |= dropPartitionByExactTimestamp(partitionTimestamp);
                continue;
            }


            long partitionCeiling = txWriter.getNextLogicalPartitionTimestamp(partitionTimestamp);
            // TTL < 0 means it's in months
            boolean shouldEvict = ttl > 0
                    ? maxTimestamp - partitionCeiling >= timestampDriver.fromHours(ttl)
                    : timestampDriver.monthsBetween(partitionCeiling, maxTimestamp) >= -ttl;
            if (shouldEvict) {
                LOG.info()
                        .$("Partition's TTL expired, evicting [table=").$(metadata.getTableToken())
                        .$(", partitionTs=").microTime(partitionTimestamp)
                        .I$();
                dropped |= dropPartitionByExactTimestamp(partitionTimestamp);
                evictedPartitionTimestamp = partitionTimestamp;
            } else {
                // Partitions are sorted by timestamp, no need to check the rest
                break;
            }
        } while (getPartitionCount() > 1);

        if (dropped) {
            commitRemovePartitionOperation();
        }
    }

    @Override
    public void forceRemovePartitions(LongList partitionTimestamps) {
        long minTimestamp = txWriter.getMinTimestamp(); // partition min timestamp
        long maxTimestamp = txWriter.getMaxTimestamp(); // partition max timestamp
        boolean firstPartitionDropped = false;
        boolean activePartitionDropped = false;

        txWriter.beginPartitionSizeUpdate();
        partitionRemoveCandidates.clear();

        int removedCount = 0;
        for (int i = 0; i < partitionTimestamps.size(); i++) {
            long timestamp = partitionTimestamps.getQuick(i);
            final int index = txWriter.getPartitionIndex(timestamp);
            if (index < 0) {
                LOG.debug().$("partition is already removed [path=").$substr(pathRootSize, path).$(", partitionTimestamp=").$ts(timestampDriver, timestamp).I$();
                continue;
            }

            removedCount++;
            if (!activePartitionDropped) {
                activePartitionDropped = timestamp == txWriter.getPartitionTimestampByTimestamp(maxTimestamp);
                if (activePartitionDropped) {
                    // Need to do this only once
                    closeActivePartition(false);
                }
            }

            firstPartitionDropped |= timestamp == txWriter.getPartitionTimestampByIndex(0);
            columnVersionWriter.removePartition(timestamp);
            txWriter.removeAttachedPartitions(timestamp);
            // Add the partition to the partition remove list that can be deleted if there are no open readers
            // after the commit
            partitionRemoveCandidates.add(timestamp, txWriter.getPartitionNameTxn(index));
        }

        if (removedCount > 0) {
            if (txWriter.getPartitionCount() > 0) {
                if (firstPartitionDropped) {
                    minTimestamp = readMinTimestamp();
                    txWriter.setMinTimestamp(minTimestamp);
                }

                if (activePartitionDropped) {
                    final int partitionIndex = txWriter.getPartitionCount() - 1;
                    long activePartitionTs = txWriter.getPartitionTimestampByIndex(partitionIndex);
                    long activePartitionRows = txWriter.getPartitionSize(partitionIndex);
                    long parquetSize = txWriter.getPartitionParquetFileSize(partitionIndex);
                    long txn = txWriter.getPartitionNameTxn(partitionIndex);
                    setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, activePartitionTs, txn);
                    try {
                        readPartitionMinMaxTimestamps(activePartitionTs, path, metadata.getColumnName(metadata.getTimestampIndex()), parquetSize, activePartitionRows);
                        maxTimestamp = attachMaxTimestamp;
                    } finally {
                        path.trimTo(pathSize);
                    }
                }

                txWriter.finishPartitionSizeUpdate(minTimestamp, maxTimestamp);
                if (activePartitionDropped) {
                    openLastPartition();
                }
                txWriter.bumpTruncateVersion();

                columnVersionWriter.commit();
                txWriter.setColumnVersion(columnVersionWriter.getVersion());
                txWriter.commit(denseSymbolMapWriters);
            } else {
                // all partitions are deleted, effectively the same as truncating the table
                rowAction = ROW_ACTION_OPEN_PARTITION;
                txWriter.resetTimestamp();
                columnVersionWriter.truncate();
                freeColumns(false);
                releaseIndexerWriters();
                txWriter.truncate(columnVersionWriter.getVersion(), denseSymbolMapWriters);
            }

            // Call O3 methods to remove check TxnScoreboard and remove partition directly
            processPartitionRemoveCandidates();
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

    public byte getPartitionFormat(int partitionIndex) {
        return txWriter.isPartitionParquet(partitionIndex) ? PartitionFormat.PARQUET : PartitionFormat.NATIVE;
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
                (avgRecordSize != 0 ? avgRecordSize : (avgRecordSize = estimateAvgRecordSize(metadata)));
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
    public @NotNull TableToken getTableToken() {
        return tableToken;
    }

    @Override
    public int getTimestampType() {
        return timestampType;
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

    public boolean isCommitDedupMode() {
        if (!isDeduplicationEnabled()) {
            return false;
        }
        return dedupMode == WalUtils.WAL_DEDUP_MODE_DEFAULT || dedupMode == WalUtils.WAL_DEDUP_MODE_UPSERT_NEW;
    }

    public boolean isCommitPlainInsert() {
        return (dedupMode == WalUtils.WAL_DEDUP_MODE_NO_DEDUP || !isDeduplicationEnabled()) && dedupMode != WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;
    }

    public boolean isCommitReplaceMode() {
        return dedupMode == WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;
    }

    public boolean isDeduplicationEnabled() {
        int tsIndex = metadata.timestampIndex;
        return tsIndex > -1 && metadata.isDedupKey(tsIndex);
    }

    public boolean isDistressed() {
        return distressed;
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

    public void markDistressed() {
        this.distressed = true;
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
        if (rowAction != ROW_ACTION_NO_TIMESTAMP) {
            timestampDriver.validateBounds(timestamp);
        }
        switch (rowAction) {
            case ROW_ACTION_NO_PARTITION:
                if (timestamp < txWriter.getMaxTimestamp()) {
                    throw CairoException.nonCritical()
                            .put("cannot insert rows out of order to non-partitioned table. Table=").put(path);
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
            case ROW_ACTION_OPEN_PARTITION:
                if (txWriter.getMaxTimestamp() == Long.MIN_VALUE) {
                    txWriter.setMinTimestamp(timestamp);
                    initLastPartition(txWriter.getPartitionTimestampByTimestamp(timestamp));
                }
                rowAction = ROW_ACTION_SWITCH_PARTITION;
                // fall thru
            case ROW_ACTION_SWITCH_PARTITION:
                bumpMasterRef();
                if (timestamp > partitionTimestampHi || timestamp < txWriter.getMaxTimestamp()) {
                    if (timestamp < txWriter.getMaxTimestamp()) {
                        return newRowO3(timestamp);
                    }

                    if (PartitionBy.isPartitioned(partitionBy)) {
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
            default:
                throw new AssertionError("Invalid row action constant");
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
                    // Don't block the queue even if the command is unknown
                    commandSubSeq.done(cursor);
                    break;
            }
        } else {
            LOG.info()
                    .$("not my command [cmdTableId=").$(cmd.getTableId())
                    .$(", cmdTableName=").$(cmd.getTableToken())
                    .$(", myTableId=").$(getMetadata().getTableId())
                    .$(", myTableName=").$(tableToken)
                    .I$();
            commandSubSeq.done(cursor);
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
            walTxnDetails = new WalTxnDetails(configuration, getWalMaxLagRows());
        }

        walTxnDetails.readObservableTxnMeta(other, transactionLogCursor, pathSize, getAppliedSeqTxn(), txWriter.getMaxTimestamp());
    }

    /**
     * Truncates table partitions leaving symbol files.
     * Used to truncate without holding Read lock on the table like in case of WAL tables.
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

        LOG.info().$("removed all partitions (soft truncated) [name=").$(tableToken).I$();
    }

    @Override
    public void removeColumn(@NotNull CharSequence name) {
        assert txWriter.getLagRowCount() == 0;

        checkDistressed();
        checkColumnName(name);

        final int index = getColumnIndex(name);
        final int type = metadata.getColumnType(index);
        final boolean isIndexed = metadata.isIndexed(index);
        String columnName = metadata.getColumnName(index);

        LOG.info().$("removing [column=").$safe(name).$(", path=").$substr(pathRootSize, path).I$();

        // check if we are moving timestamp from a partitioned table
        final int timestampIndex = metadata.getTimestampIndex();
        boolean timestamp = (index == timestampIndex);

        if (timestamp && PartitionBy.isPartitioned(partitionBy)) {
            throw CairoException.nonCritical().put("cannot remove timestamp from partitioned table");
        }

        commit();

        metadata.removeColumn(index);
        if (timestamp) {
            metadata.clearTimestampIndex();
        }
        rewriteAndSwapMetadata(metadata);

        try {
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

            // remove column files
            removeColumnFiles(index, columnName, type, isIndexed);
            clearTodoAndCommitMetaStructureVersion();

            finishColumnPurge();

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }
            LOG.info().$("REMOVED column '").$safe(name).$('[').$(ColumnType.nameOf(type)).$("]' from ").$substr(pathRootSize, path).$();
        } catch (CairoError err) {
            throw err;
        } catch (Throwable th) {
            throwDistressException(th);
        }
    }

    @Override
    public boolean removePartition(long timestamp) {
        partitionRemoveCandidates.clear();
        if (!PartitionBy.isPartitioned(partitionBy)) {
            return false;
        }

        // commit changes, there may be uncommitted rows of any partition
        commit();

        // Handle split partitions.
        // One logical partition may be split into multiple physical partitions.
        // For example, partition daily '2024-02-24' can be stored as 2 pieces '2024-02-24' and '2024-02-24T12'
        long logicalPartitionTimestampToDelete = txWriter.getLogicalPartitionTimestamp(timestamp);
        int partitionIndex = txWriter.findAttachedPartitionRawIndexByLoTimestamp(logicalPartitionTimestampToDelete);
        if (partitionIndex < 0) {
            // A partition slit can exist without the partition itself.
            // For example, it is allowed to have partition '2024-02-24T12' without partition '2024-02-24'
            // To delete all the splits, start from the index that is next or equal to the timestamp parameter.
            partitionIndex = -partitionIndex - 1;
        }
        partitionIndex /= LONGS_PER_TX_ATTACHED_PARTITION;

        boolean dropped = false;
        long partitionTimestamp;
        while (partitionIndex < txWriter.getPartitionCount() &&
                txWriter.getLogicalPartitionTimestamp(
                        partitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex)
                ) == logicalPartitionTimestampToDelete) {
            dropped |= dropPartitionByExactTimestamp(partitionTimestamp);
        }

        if (dropped) {
            commitRemovePartitionOperation();
        }
        return dropped;
    }

    @Override
    public void renameColumn(
            @NotNull CharSequence name,
            @NotNull CharSequence newName,
            SecurityContext securityContext
    ) {
        checkDistressed();
        checkColumnName(newName);

        final int index = getColumnIndex(name);
        final int type = metadata.getColumnType(index);
        final boolean isIndexed = metadata.isIndexed(index);
        String columnName = metadata.getColumnName(index);

        LOG.info().$("renaming column '").$safe(columnName).$('[')
                .$(ColumnType.nameOf(type)).$("]' to '").$safe(newName)
                .$("' in ").$substr(pathRootSize, path).$();

        commit();

        String newColumnName = null;

        try {
            metadata.renameColumn(columnName, newName);
            newColumnName = metadata.getColumnName(index);
            rewriteAndSwapMetadata(metadata);

            // rename column files has to be done before _todo is removed
            hardLinkAndPurgeColumnFiles(columnName, index, isIndexed, newColumnName, type);

            // commit to _txn file
            bumpMetadataAndColumnStructureVersion();
        } catch (CairoException e) {
            throwDistressException(e);
        }

        // remove _todo as last step, after the commit.
        // if anything fails before the commit, meta file will be reverted
        clearTodoLog();

        try {

            // Call finish purge to remove old column files before renaming them in metadata
            finishColumnPurge();

            if (index == metadata.getTimestampIndex()) {
                designatedTimestampColumnName = newColumnName;
            }

            if (securityContext != null) {
                ddlListener.onColumnRenamed(securityContext, tableToken, columnName, newColumnName);
            }

            try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                metadataRW.hydrateTable(metadata);
            }

            LOG.info().$("RENAMED column '").$safe(columnName).$("' to '").$safe(newColumnName).$("' from ").$substr(pathRootSize, path).$();
        } catch (Throwable e) {
            handleHousekeepingException(e);
        }
    }

    @Override
    public void renameTable(@NotNull CharSequence fromTableName, @NotNull CharSequence toTableName) {
        // table writer is not involved in concurrent table rename, the `fromTableName` must
        // always match tableWriter's table name
        LOG.debug().$("renaming table [path=").$substr(pathRootSize, path).$(", seqTxn=").$(txWriter.getSeqTxn()).I$();
        try {
            overwriteTableNameFile(path, ddlMem, ff, toTableName);
        } finally {
            path.trimTo(pathSize);
        }
        // Record column structure version bump in txn file for WAL sequencer structure version to match the writer structure version.
        bumpColumnStructureVersion();
    }

    @Override
    public void rollback() {
        checkDistressed();
        if (o3InError || inTransaction()) {
            try {
                LOG.info().$("tx rollback [name=").$(tableToken).I$();
                partitionRemoveCandidates.clear();
                o3CommitBatchTimestampMin = Long.MAX_VALUE;
                if ((masterRef & 1) != 0) {
                    // Potentially failed in row writing like putSym() call.
                    // This can mean that the writer state is corrupt. Force re-open writer with next transaction
                    LOG.critical().$("detected line append failure, writer marked as distressed [table=").$(tableToken).I$();
                    distressed = true;
                    checkDistressed();
                }
                freeColumns(false);
                txWriter.unsafeLoadAll();
                rollbackIndexes();
                rollbackSymbolTables(true);
                columnVersionWriter.readUnsafe();
                closeActivePartition(false);
                purgeUnusedPartitions();
                configureAppendPosition();
                o3InError = false;
                // when we rolled transaction back, hasO3() has to be false
                o3MasterRef = -1;
                LOG.info().$("tx rollback complete [table=").$(tableToken).I$();
                processCommandQueue(false);
                metrics.tableWriterMetrics().incrementRollbacks();
            } catch (Throwable e) {
                LOG.critical().$("could not perform rollback [table=").$(tableToken).$(", msg=").$(e).I$();
                distressed = true;
            }
            // If it's a manual rollback call, throw exception to indicate that the rollback was not successful
            // and the writer must be closed.
            checkDistressed();
        }
    }

    public void setExtensionListener(ExtensionListener listener) {
        txWriter.setExtensionListener(listener);
    }

    public void setLifecycleManager(LifecycleManager lifecycleManager) {
        this.lifecycleManager = lifecycleManager;
    }

    @Override
    public void setMatViewRefresh(
            int refreshType,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable CharSequence timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        assert tableToken.isMatView();

        final MatViewDefinition oldDefinition = engine.getMatViewGraph().getViewDefinition(tableToken);
        if (oldDefinition == null) {
            throw CairoException.nonCritical().put("could not find definition [view=").put(tableToken.getTableName()).put(']');
        }

        final MatViewDefinition newDefinition = oldDefinition.updateRefreshParams(
                refreshType,
                timerInterval,
                timerUnit,
                timerStartUs,
                Chars.toString(timerTimeZone),
                periodLength,
                periodLengthUnit,
                periodDelay,
                periodDelayUnit
        );
        updateMatViewDefinition(newDefinition);
    }

    @Override
    public void setMatViewRefreshLimit(int limitHoursOrMonths) {
        assert tableToken.isMatView();

        final MatViewDefinition oldDefinition = engine.getMatViewGraph().getViewDefinition(tableToken);
        if (oldDefinition == null) {
            throw CairoException.nonCritical().put("could not find definition [view=").put(tableToken.getTableName()).put(']');
        }

        final MatViewDefinition newDefinition = oldDefinition.updateRefreshLimit(limitHoursOrMonths);
        updateMatViewDefinition(newDefinition);
    }

    @Override
    public void setMatViewRefreshTimer(long startUs, int interval, char unit) {
        assert tableToken.isMatView();

        final MatViewDefinition oldDefinition = engine.getMatViewGraph().getViewDefinition(tableToken);
        if (oldDefinition == null) {
            throw CairoException.nonCritical().put("could not find definition [view=").put(tableToken.getTableName()).put(']');
        }

        final MatViewDefinition newDefinition = oldDefinition.updateTimer(interval, unit, startUs);
        updateMatViewDefinition(newDefinition);
    }

    @Override
    public void setMetaMaxUncommittedRows(int maxUncommittedRows) {
        commit();
        metadata.setMaxUncommittedRows(maxUncommittedRows);
        writeMetadataToDisk();
    }

    @Override
    public void setMetaO3MaxLag(long o3MaxLagUs) {
        commit();
        metadata.setO3MaxLag(o3MaxLagUs);
        writeMetadataToDisk();
    }

    @Override
    public void setMetaTtl(int ttlHoursOrMonths) {
        commit();
        metadata.setTtlHoursOrMonths(ttlHoursOrMonths);
        writeMetadataToDisk();
    }

    public void setSeqTxn(long seqTxn) {
        assert txWriter.getLagRowCount() == 0 && txWriter.getLagTxnCount() == 0;
        txWriter.setSeqTxn(seqTxn);
    }

    public long size() {
        // This is an uncommitted row count
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
        // Reopen the last partition if we've closed it.
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
     * Does not accept structure changes, e.g., equivalent to tick(false)
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
     *                                         structure changes like a column drop, rename
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
     * Truncates table including symbol tables. When the operation is unsuccessful, it throws CairoException.
     * With that truncate can be retried or alternatively, the table can be closed. The outcome of any other operation
     * with the table is undefined and likely to cause a segmentation fault. When the table re-opens, any partial
     * truncate will be retried.
     */
    @Override
    public final void truncate() {
        truncate(false);
    }

    /**
     * Truncates table, but keeps symbol tables. When the operation is unsuccessful, it throws CairoException.
     * With that truncate can be retried or alternatively, the table can be closed. The outcome of any other operation
     * with the table is undefined and likely to cause a segmentation fault. When the table re-opens, any partial
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
     * Eagerly sets up writer instance. Otherwise, the writer will initialize lazily. Invoking this method could improve
     * the performance of some applications. UDP receivers use this in order to avoid initial receive buffer contention.
     */
    public void warmUp() {
        Row r = newRow(Math.max(TIMESTAMP_EPOCH, txWriter.getMaxTimestamp()));
        try {
            for (int i = 0; i < columnCount; i++) {
                r.putByte(i, (byte) 0);
            }
        } finally {
            r.cancel();
        }
    }

    private static void clearMemColumnShifts(ObjList<MemoryCARW> memColumns) {
        for (int i = 0, n = memColumns.size(); i < n; i++) {
            MemoryCARW col = memColumns.get(i);
            if (col != null) {
                col.shiftAddressRight(0);
            }
        }
    }

    private static void closeRemove(FilesFacade ff, long fd, LPSZ path) {
        if (!ff.closeRemove(fd, path)) {
            throw CairoException.critical(ff.errno()).put("cannot remove ").put(path);
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
                case ColumnType.DECIMAL8:
                    nullers.add(() -> dataMem.putByte(Byte.MIN_VALUE));
                    break;
                case ColumnType.DECIMAL16:
                    nullers.add(() -> dataMem.putShort(Short.MIN_VALUE));
                    break;
                case ColumnType.DECIMAL32:
                    nullers.add(() -> dataMem.putInt(Integer.MIN_VALUE));
                    break;
                case ColumnType.DECIMAL64:
                    nullers.add(() -> dataMem.putLong(Long.MIN_VALUE));
                    break;
                case ColumnType.DECIMAL128:
                    nullers.add(() -> dataMem.putDecimal128(Long.MIN_VALUE, -1));
                    break;
                case ColumnType.DECIMAL256:
                    nullers.add(() -> dataMem.putDecimal256(Long.MIN_VALUE, -1, -1, -1));
                    break;
                default:
                    nullers.add(NOOP);
            }
        }
    }

    private static void linkFile(FilesFacade ff, LPSZ from, LPSZ to) {
        if (ff.exists(from)) {
            if (ff.hardLink(from, to) == FILES_RENAME_OK) {
                LOG.debug().$("renamed [from=").$(from).$(", to=").$(to).I$();
                return;
            } else if (ff.exists(to)) {
                LOG.info().$("rename destination file exists, assuming previously failed rename attempt [path=").$(to).I$();
                try {
                    ff.remove(to);
                } catch (CairoException e) {
                    if (Os.isWindows() && ff.errno() == CairoException.ERRNO_ACCESS_DENIED_WIN) {
                        // On Windows, it's not possible to delete a link if the original file is open.
                        // Here we assume that it's exactly what we need, linking the correct from/to paths.
                        // There is no good way to verify that, but there is no hypothetical scenario found
                        // when this is false.
                        LOG.info().$("cannot delete file to create link with the same name," +
                                " assuming already correctly linked [path=").$(to).$(", linkSrc=").$(from).I$();
                        return;
                    } else {
                        throw e;
                    }
                }
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

    private static void openMetaFile(FilesFacade ff, Path path, int rootLen, MemoryMR ddlMem, TableWriterMetadata metadata) {
        path.concat(META_FILE_NAME);
        try (ddlMem) {
            ddlMem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
            metadata.reload(path, ddlMem);
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
            boolean isDedupKey,
            long columnNameTxn,
            int replaceColumnIndex,
            TableWriterMetadata metadata
    ) {
        // If this is a column type change, find the index of the very first column this one replaces
        int prevReplaceColumnIndex = replaceColumnIndex;
        while (prevReplaceColumnIndex > -1) {
            replaceColumnIndex = prevReplaceColumnIndex;
            prevReplaceColumnIndex = metadata.getReplacingColumnIndex(replaceColumnIndex);
        }

        metadata.addColumn(
                columnName,
                columnType,
                isIndexed,
                indexValueBlockCapacity,
                metadata.getColumnCount(),
                symbolCapacity,
                isDedupKey,
                replaceColumnIndex,
                symbolCacheFlag
        );

        rewriteAndSwapMetadata(metadata);

        // don't create a symbol writer when column conversion happens, it should be created before the conversion
        if (replaceColumnIndex < 0) {
            if (ColumnType.isSymbol(columnType)) {
                try {
                    createSymbolMapWriter(columnName, columnNameTxn, symbolCapacity, symbolCacheFlag, metadata.getColumnCount() - 1);
                } catch (CairoException e) {
                    try {
                        recoverFromSymbolMapWriterFailure(columnName);
                    } catch (CairoException e2) {
                        LOG.error().$("DOUBLE ERROR: 1st: {").$((Sinkable) e).$('}').$();
                        throwDistressException(e2);
                    }
                    throw e;
                }
            } else {
                // maintain the sparse list of symbol writers
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

    private void appendBuffer(long fd, long address, long len) {
        if (ff.append(fd, address, len) != len) {
            throw CairoException.critical(ff.errno()).put("cannot append data [fd=")
                    .put(fd).put(", len=").put(len).put(']');
        }
    }

    private long applyFromWalLagToLastPartition(long commitToTimestamp, boolean allowPartial) {
        long lagMinTimestamp = txWriter.getLagMinTimestamp();
        if (!isCommitDedupMode()
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
                            Vect.BIN_SEARCH_SCAN_DOWN
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
        return !isCommitDedupMode()
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

        dFile(partitionPath, columnName, columnNameTxn);
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
        iFile(partitionPath, columnName, columnNameTxn);
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
                    dFile(partitionPath, columnName, columnNameTxn);
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

    private void attachPartitionCheckSymbolColumn(
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

        int pathLen = partitionPath.size();
        dFile(partitionPath, columnName, columnNameTxn);
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
                    throw CairoException.fileNotFound()
                            .put("Symbol index value file does not exist [file=")
                            .put(partitionPath)
                            .put(']');
                }
                keyFileName(partitionPath.trimTo(pathLen), columnName, columnNameTxn);
                if (!ff.exists(partitionPath.$())) {
                    throw CairoException.fileNotFound()
                            .put("Symbol index key file does not exist [file=")
                            .put(partitionPath)
                            .put(']');
                }
            }
        } finally {
            ff.close(fd);
        }
    }

    private void attachPartitionPinColumnVersions(long pUtf8NameZ, int type) {
        if (notDots(pUtf8NameZ) && type == DT_FILE) {
            tmpDirectUtf8StringZ.of(pUtf8NameZ);
            int firstDot = Utf8s.indexOfAscii(tmpDirectUtf8StringZ, '.');
            if (firstDot != -1) {
                utf16Sink.clear();
                utf16Sink.put(tmpDirectUtf8StringZ, 0, firstDot);
                // all our column files have .d extensions
                int columnIndex = metadata.getColumnIndexQuiet(utf16Sink);
                if (columnIndex != -1) {
                    // not a random file, we have column by this name
                    int lastDot = Utf8s.lastIndexOfAscii(tmpDirectUtf8StringZ, '.');
                    int len = Utf8s.length(tmpDirectUtf8StringZ);
                    if (lastDot > 1 && lastDot < len - 1) {
                        // we are rejecting 'abc', '.abc' and 'abc.', but accepting 'a.xxx'
                        try {
                            long nameTxn = Numbers.parseLong(tmpDirectUtf8StringZ, lastDot + 1, len);
                            if (nameTxn > 0) {
                                // check that this is xxx.d.234
                                if (tmpDirectUtf8StringZ.byteAt(lastDot - 1) == 'd') {
                                    columnVersionWriter.upsert(
                                            attachPartitionTimestamp,
                                            columnIndex,
                                            nameTxn,
                                            // column tops will have been already read from the "cv" file, we just need
                                            // to upsert the nameTxn and keep the columnTop unchanged
                                            getColumnTop(attachPartitionTimestamp, columnIndex, 0)
                                    );
                                }
                            }
                        } catch (NumericException ignore) {
                        }
                    }
                }
            }
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
                attachMetadata = new TableWriterMetadata(tableToken);
            }
            attachMetaMem.smallFile(ff, detachedPath.$(), MemoryTag.MMAP_TABLE_WRITER);
            attachMetadata.reload(detachedPath, attachMetaMem);

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
                    LOG.info().$("detached partition has column deleted while the table has the same column alive [tableName=").$(tableToken)
                            .$(", column=").$safe(columnName)
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

    private int calculateInsertTransactionBlock(long seqTxn, TableWriterPressureControl pressureControl) {
        if (txWriter.getLagRowCount() > 0) {
            pressureControl.updateInflightTxnBlockLength(
                    1,
                    Math.max(1, walTxnDetails.getSegmentRowHi(seqTxn) - walTxnDetails.getSegmentRowLo(seqTxn))
            );
            return 1;
        }
        return walTxnDetails.calculateInsertTransactionBlock(seqTxn, pressureControl, getWalMaxLagRows());
    }

    private boolean canSquashOverwritePartitionTail(int partitionIndex) {
        long fromTxn = txWriter.getPartitionNameTxn(partitionIndex);
        if (fromTxn < 0) {
            fromTxn = 0;
        }
        long toTxn = txWriter.getTxn();
        if (partitionIndex + 1 < txWriter.getPartitionCount()) {
            // If the next partition is a split partition part of same logical partition
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
        if (!isValidColumnName(name, configuration.getMaxFileNameLength())) {
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
                        tableToken.getTableName() + ", tableDir=" + tableToken.getDirName() + "]");
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

    private void clearMemColumnShifts() {
        clearMemColumnShifts(o3MemColumns1);
        clearMemColumnShifts(o3MemColumns2);
        memColumnShifted = false;
    }

    private void clearO3() {
        this.o3MasterRef = -1; // clears o3 flag, hasO3() will be returning false
        rowAction = ROW_ACTION_SWITCH_PARTITION;
        // transaction log is either not required or pending
        activeColumns = columns;
        activeNullSetters = nullSetters;
    }

    private void clearTodoAndCommitMeta() {
        try {
            bumpMetadataVersion();
        } catch (CairoException e) {
            throwDistressException(e);
        }

        clearTodoLog();
    }

    private void clearTodoAndCommitMetaStructureVersion() {
        try {
            bumpMetadataAndColumnStructureVersion();
        } catch (CairoException e) {
            throwDistressException(e);
        }

        clearTodoLog();
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
            // ensure the file is closed with the correct length
            todoMem.jumpTo(40);
            todoMem.sync(false);
        } catch (Throwable th) {
            // if we failed to clear _todo_, it's ok, it will be ignored
            // because the txn inside _todo_ is out of date.
            handleHousekeepingException(th);
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

    /**
     * Commits newly added rows of data. This method updates transaction file with pointers to the end of appended data.
     * <p>
     * <b>Pending rows</b>
     * <p>This method will cancel pending rows by calling {@link #rowCancel()}. Data in the partially appended row will be lost.</p>
     *
     * @param o3MaxLag if > 0 then do a partial commit, leaving the rows within the lag in a new uncommitted transaction
     */
    private void commit(long o3MaxLag) {
        checkDistressed();
        physicallyWrittenRowsSinceLastCommit.set(0);

        if (o3InError) {
            rollback();
            return;
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
                    return;
                } else if (o3MaxLag > 0) {
                    // It is possible that O3 commit will create partition just before
                    // the last one, leaving the last partition row count 0 when doing ic().
                    // That's when the data from the last partition is moved to in-memory lag.
                    // One way to detect this is to check if the index of the "last" partition is not
                    // the last partition in the attached partition list.
                    if (reconcileOptimisticPartitions()) {
                        this.lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
                        this.partitionTimestampHi = txWriter.getCurrentPartitionMaxTimestamp(txWriter.getMaxTimestamp());
                        openLastPartition();
                    }
                }
            } else if (noOpRowCount > 0) {
                LOG.critical()
                        .$("o3 ignoring write on read-only partition [table=").$(tableToken)
                        .$(", timestamp=").$ts(timestampDriver, lastOpenPartitionTs)
                        .$(", numRows=").$(noOpRowCount)
                        .$();
            }


            final long committedRowCount = txWriter.unsafeCommittedFixedRowCount() + txWriter.unsafeCommittedTransientRowCount();
            final long rowsAdded = txWriter.getRowCount() - committedRowCount;

            commit00();
            housekeep();
            metrics.tableWriterMetrics().addCommittedRows(rowsAdded);
            if (!o3) {
                // If `o3`, the metric is tracked inside `o3Commit`, possibly async.
                addPhysicallyWrittenRows(rowsAdded);
            }

            noOpRowCount = 0L;

            LOG.debug().$("table ranges after the commit [table=").$(tableToken)
                    .$(", minTs=").$ts(timestampDriver, txWriter.getMinTimestamp())
                    .$(", maxTs=").$ts(timestampDriver, txWriter.getMaxTimestamp())
                    .I$();
        }
    }

    private void commit00() {
        updateIndexes();
        syncColumns();
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.commit(denseSymbolMapWriters);
        // Bookmark masterRef to track how many rows is in uncommitted state
        this.committedMasterRef = masterRef;
    }

    private void commitRemovePartitionOperation() {
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.commit(denseSymbolMapWriters);
        processPartitionRemoveCandidates();
    }

    private void configureAppendPosition() {
        final boolean partitioned = PartitionBy.isPartitioned(partitionBy);
        if (this.txWriter.getMaxTimestamp() > Long.MIN_VALUE || !partitioned) {
            initLastPartition(this.txWriter.getMaxTimestamp());
            if (partitioned) {
                partitionTimestampHi = txWriter.getCurrentPartitionMaxTimestamp(txWriter.getMaxTimestamp());
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
            dataMem = Vm.getPMARInstance(configuration);
            o3DataMem1 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);
            o3DataMem2 = Vm.getCARWInstance(o3ColumnMemorySize, configuration.getO3MemMaxPages(), MemoryTag.NATIVE_O3);

            if (ColumnType.isVarSize(type)) {
                auxMem = Vm.getPMARInstance(configuration);
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
                    final int symbolIndexInTxWriter = denseSymbolMapWriters.size();
                    long symbolTableNameTxn = columnVersionWriter.getSymbolTableNameTxn(i);
                    SymbolMapWriter symbolMapWriter = new SymbolMapWriter(
                            configuration,
                            path.trimTo(pathSize),
                            metadata.getColumnName(i),
                            symbolTableNameTxn,
                            txWriter.getSymbolValueCount(symbolIndexInTxWriter),
                            symbolIndexInTxWriter,
                            txWriter,
                            i
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

    private int copyOverwrite(Path to) {
        int res = ff.copy(other.$(), to.$());
        if (Os.isWindows() && res == -1 && ff.errno() == Files.WINDOWS_ERROR_FILE_EXISTS) {
            // Windows throws an error the destination file already exists, other platforms do not
            if (!ff.removeQuiet(to.$())) {
                // If the file is open, return here so that errno is 5 in the error message
                return -1;
            }
            return ff.copy(other.$(), to.$());
        }
        return res;
    }

    private void copyPartitionIndexFiles(long partitionTimestamp, int partitionDirLen, int newPartitionDirLen) {
        try {
            final int columnCount = metadata.getColumnCount();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final String columnName = metadata.getColumnName(columnIndex);
                if (ColumnType.isSymbol(metadata.getColumnType(columnIndex)) && metadata.isIndexed(columnIndex)) {
                    final long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);

                    // no data in partition for this column
                    if (columnTop == -1) {
                        continue;
                    }

                    final long columnNameTxn = getColumnNameTxn(partitionTimestamp, columnIndex);

                    BitmapIndexUtils.keyFileName(path.trimTo(partitionDirLen), columnName, columnNameTxn);
                    BitmapIndexUtils.keyFileName(other.trimTo(newPartitionDirLen), columnName, columnNameTxn);
                    if (ff.copy(path.$(), other.$()) < 0) {
                        throw CairoException.critical(ff.errno())
                                .put("could not copy index key file [table=")
                                .put(tableToken.getTableName())
                                .put(", column=")
                                .put(columnName)
                                .put(']');
                    }

                    BitmapIndexUtils.valueFileName(path.trimTo(partitionDirLen), columnName, columnNameTxn);
                    BitmapIndexUtils.valueFileName(other.trimTo(newPartitionDirLen), columnName, columnNameTxn);
                    if (ff.copy(path.$(), other.$()) < 0) {
                        throw CairoException.critical(ff.errno())
                                .put("could not copy index value file [table=")
                                .put(tableToken.getTableName())
                                .put(", column=")
                                .put(columnName)
                                .put(']');
                    }
                }
            }
        } catch (CairoException e) {
            LOG.error().$("could not copy index files [table=").$(tableToken)
                    .$(", partition=").$ts(timestampDriver, partitionTimestamp)
                    .$(", error=").$safe(e.getMessage()).I$();

            // rollback
            if (!ff.rmdir(other.trimTo(newPartitionDirLen).slash())) {
                LOG.error().$("could not remove partition dir [path=").$(other).I$();
            }
            throw e;
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    /**
     * Creates bitmap index files for a column. This method uses primary column instance as a temporary tool to
     * append index data. Therefore, it must be called before the primary column is initialized.
     *
     * @param columnName              column name
     * @param indexValueBlockCapacity approximate number of values per index key
     * @param plen                    path length. This is used to trim the shared path object to.
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
                // looks like we could not create the key file properly;
                // let's not leave a half-baked file sitting around
                LOG.error()
                        .$("could not create index [name=").$(path)
                        .$(", msg=").$safe(e.getFlyweightMessage())
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

    private void createSymbolMapWriter(
            CharSequence name,
            long columnNameTxn,
            int symbolCapacity,
            boolean symbolCacheFlag,
            int columnIndex
    ) {
        MapWriter.createSymbolMapFiles(ff, ddlMem, path, name, columnNameTxn, symbolCapacity, symbolCacheFlag);
        SymbolMapWriter w = new SymbolMapWriter(
                configuration,
                path,
                name,
                columnNameTxn,
                0,
                denseSymbolMapWriters.size(),
                txWriter,
                columnIndex
        );

        try {
            // In case, there are some dirty files left from rolled back transaction
            // clean the newly created symbol files.
            w.truncate();
        } catch (Throwable t) {
            // oh, well, we tried and it failed. this can happen if there is e.g., I/O issue.
            // we can't do much about it but make sure we close the writer to avoid leaks
            w.close();
            throw t;
        }

        denseSymbolMapWriters.add(w);
        symbolMapWriters.extendAndSet(columnCount, w);
    }

    private boolean createWalSymbolMapping(SymbolMapDiff symbolMapDiff, int columnIndex, IntList symbolMap) {
        final int cleanSymbolCount = symbolMapDiff.getCleanSymbolCount();
        symbolMap.setPos(symbolMapDiff.getRecordCount());

        // This is defensive. It validates that all the symbols used in WAL are set in SymbolMapDiff
        symbolMap.setAll(symbolMapDiff.getRecordCount(), -1);
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
            symbolMap.set(entry.getKey() - cleanSymbolCount, newKey);
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

                // move count+1 rows, to make sure index column remains n+1
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
                // WAL format has timestamp written as 2 LONGs per record in so-called timestamp index data structure.
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
            long lagMemOffset = lagRows > 0 ? (txWriter.getTransientRowCount() - getColumnTop(columnIndex)) << shl : 0;
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
            final long lagAuxOffset = lagRows > 0 ? columnTypeDriver.getAuxVectorOffset(txWriter.getTransientRowCount() - getColumnTop(columnIndex)) : 0;
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
                        colAuxMemAddr = TableUtils.mapRO(ff, colAuxMem.getFd(), colAuxMemRequiredSize + alignedExtraLen, alignedOffset, MemoryTag.MMAP_TABLE_WRITER);
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
                int shl = ColumnType.pow2SizeOf(timestampType);
                MemoryMA srcDataMem = getPrimaryColumn(columnIndex);
                // this cannot have "top"
                long srcFixOffset = committedTransientRowCount << shl;
                long srcFixLen = transientRowsAdded << shl;
                long alignedExtraLen;
                long address = srcDataMem.map(srcFixOffset, srcFixLen);
                boolean locallyMapped = address == 0;

                // column could not provide the necessary length of buffer
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

                    // adjust the append-position of the index column to
                    // maintain n+1 number of entries
                    long rowLimit = columnTypeDriver.getAuxVectorSize(existingLagRows + copyToLagRowCount);
                    dstAuxMem.jumpTo(rowLimit);

                    // move count+1 rows, to make sure index column remains n+1
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

                // the data is copied back to the start of the buffer, no need to set size first
            } else {
                MemoryCR o3SrcDataMem = o3Columns.get(getPrimaryColumnIndex(columnIndex));

                // Special case, designated timestamp column
                // Move values and set index to 0..copyToLagRowCount
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

                                long lagAuxOffset = driver.getAuxVectorOffset(roLo);
                                long lagAuxSize = driver.getAuxVectorSize(lagRows);
                                long lagAuxKeyAddrRaw = mapAppendColumnBuffer(columns.get(getSecondaryColumnIndex(i)), lagAuxOffset, lagAuxSize, false);
                                long lagAuxKeyAddr = Math.abs(lagAuxKeyAddrRaw);

                                long lagVarDataOffset = driver.getDataVectorOffset(lagAuxKeyAddr, 0);
                                long lagVarDataSize = driver.getDataVectorSize(lagAuxKeyAddr, 0, lagRows - 1);
                                long lagVarDataAddrRaw = mapAppendColumnBuffer(columns.get(getPrimaryColumnIndex(i)), lagVarDataOffset, lagVarDataSize, false);
                                long lagVarDataAddr = Math.abs(lagVarDataAddrRaw);
                                // Aux points into the var buffer as if it's mapped from 0 row.
                                // Compensate mapped with offset address of var buffer by subtracting lagVarDataOffset
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
            long deduplicatedRowCount = Vect.dedupSortedTimestampIndexIntKeysChecked(
                    indexSrcAddr,
                    longIndexLength,
                    indexDstAddr,
                    tempIndexAddr,
                    dedupKeyIndex,
                    DedupColumnCommitAddresses.getAddress(dedupCommitAddr)
            );

            LOG.info().$("WAL dedup sorted commit index [table=").$(tableToken)
                    .$(", totalRows=").$(longIndexLength)
                    .$(", lagRows=").$(lagRows)
                    .$(", dups=")
                    .$(deduplicatedRowCount > 0 ? longIndexLength - deduplicatedRowCount : 0)
                    .I$();
            return deduplicatedRowCount;
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

                // Pass column index as -1 when it's a designated timestamp column to o3 move method
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
        Misc.free(parquetDecoder);
        Misc.free(parquetStatBuffers);
        Misc.free(parquetColumnIdsAndTypes);
        Misc.free(segmentCopyInfo);
        Misc.free(walTxnDetails);
        Misc.free(blockFileWriter);
        tempDirectMemList = Misc.free(tempDirectMemList);
        if (segmentFileCache != null) {
            segmentFileCache.closeWalFiles();
        }
        updateOperatorImpl = Misc.free(updateOperatorImpl);
        convertOperatorImpl = Misc.free(convertOperatorImpl);
        dropIndexOperator = null;
        noOpRowCount = 0L;
        lastOpenPartitionTs = Long.MIN_VALUE;
        lastOpenPartitionIsReadOnly = false;
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
            LOG.info().$("closed [table=").$(tableToken).I$();
        }
    }

    private boolean dropPartitionByExactTimestamp(long timestamp) {
        final long minTimestamp = txWriter.getMinTimestamp(); // table min timestamp
        final long maxTimestamp = txWriter.getMaxTimestamp(); // table max timestamp

        final int index = txWriter.getPartitionIndex(timestamp);
        if (index < 0) {
            LOG.error().$("partition is already removed [path=").$substr(pathRootSize, path).$(", partitionTimestamp=").$ts(timestampDriver, timestamp).I$();
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
                final long parquetSize = txWriter.getPartitionParquetFileSize(prevIndex);
                prevTimestamp = txWriter.getPartitionTimestampByIndex(prevIndex);
                newTransientRowCount = txWriter.getPartitionSize(prevIndex);
                try {
                    setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, prevTimestamp, txWriter.getPartitionNameTxn(prevIndex));
                    readPartitionMinMaxTimestamps(prevTimestamp, path, metadata.getColumnName(metadata.getTimestampIndex()), parquetSize, newTransientRowCount);
                    nextMaxTimestamp = attachMaxTimestamp;
                } finally {
                    path.trimTo(pathSize);
                }
            }

            // NOTE: this method should not commit to _txn file
            // In case multiple partition parts are deleted, they should be deleted atomically
            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.finishPartitionSizeUpdate(index == 0 ? Long.MAX_VALUE : txWriter.getMinTimestamp(), nextMaxTimestamp);
            txWriter.bumpTruncateVersion();
            columnVersionWriter.removePartition(timestamp);
            columnVersionWriter.replaceInitialPartitionRecords(txWriter.getLastPartitionTimestamp(), txWriter.getTransientRowCount());

            // No need to truncate before, files to be deleted.
            closeActivePartition(false);

            if (index != 0) {
                openPartition(prevTimestamp, newTransientRowCount);
                setAppendPosition(newTransientRowCount, false);
            } else {
                rowAction = ROW_ACTION_OPEN_PARTITION;
            }
        } else {
            // when we want to delete first partition we must find out minTimestamp from
            // the next partition if it exists, or the next partition, and so on
            //
            // when somebody removed data directories manually and then attempts to tidy
            // up metadata with logical partition delete, we have to uphold the effort and
            // re-compute table size and its minTimestamp from what remains on disk

            // find out if we are removing min partition
            long nextMinTimestamp = minTimestamp;
            if (timestamp == txWriter.getPartitionTimestampByIndex(0)) {
                nextMinTimestamp = readMinTimestamp();
            }

            // NOTE: this method should not commit to _txn file
            // In case multiple partition parts are deleted, they should be deleted atomically
            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.setMinTimestamp(nextMinTimestamp);
            txWriter.finishPartitionSizeUpdate(nextMinTimestamp, txWriter.getMaxTimestamp());
            txWriter.bumpTruncateVersion();
            columnVersionWriter.removePartition(timestamp);
        }

        partitionRemoveCandidates.add(timestamp, partitionNameTxn);
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
                timestampType,
                partitionBy,
                asyncOnly,
                getTruncateVersion(),
                getTxn()
        );
        purgingOperator.clear();
    }

    private void finishO3Append(long o3LagRowCount) {
        if (denseIndexers.size() == 0) {
            populateDenseIndexerList();
        }
        path.trimTo(pathSize);
        // Alright, we finished updating partitions. Now we need to get this writer instance into
        // a consistent state.
        //
        // We start with ensuring append memory is in ready-to-use state. When max timestamp changes, we need to
        // move append memory to a new set of files. Otherwise, we stay on the same set but advance to append position.
        avoidIndexOnCommit = o3ErrorCount.get() == 0;
        if (o3LagRowCount == 0) {
            clearO3();
            LOG.debug().$("lag segment is empty").$();
        } else {
            // adjust O3 master ref so that virtual row count becomes equal to the value of "o3LagRowCount"
            this.o3MasterRef = this.masterRef - o3LagRowCount * 2 + 1;
            LOG.debug().$("adjusted [o3RowCount=").$(getO3RowCount0()).I$();
        }
    }

    private void finishO3Commit(long partitionTimestampHiLimit) {
        if (!o3InError) {
            updateO3ColumnTops();
        }
        if (!isEmptyTable() && (isLastPartitionClosed() || partitionTimestampHi > partitionTimestampHiLimit)) {
            openPartition(txWriter.getLastPartitionTimestamp(), txWriter.getTransientRowCount());
        }

        // Data is written out successfully, however, we can still fail to set append position, for
        // example, when we ran out of address space and new page cannot be mapped. The "allocate" calls here
        // ensure we can trigger this situation in tests. We should perhaps align our data such that setAppendPosition()
        // will attempt to mmap new page and fail... Then we can remove the 'true' parameter
        try {
            // Set append position if this commit did not result in full table truncate
            // which is possible with replace commits.
            if (txWriter.getTransientRowCount() > 0) {
                setAppendPosition(txWriter.getTransientRowCount(), !metadata.isWalEnabled());
            }
        } catch (Throwable e) {
            LOG.critical().$("data is committed but writer failed to update its state `").$(e).$('`').$();
            distressed = true;
            throw e;
        }

        metrics.tableWriterMetrics().incrementO3Commits();
    }

    private Utf8Sequence formatPartitionForTimestamp(long partitionTimestamp, long nameTxn) {
        utf8Sink.clear();
        setSinkForNativePartition(utf8Sink, timestampType, partitionBy, partitionTimestamp, nameTxn);
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

    @NotNull
    private DirectLongList getTempDirectLongList(long capacity) {
        if (tempDirectMemList == null) {
            tempDirectMemList = new DirectLongList(capacity, MemoryTag.NATIVE_TABLE_WRITER);
            tempDirectMemList.zero();
            return tempDirectMemList;
        }
        tempDirectMemList.clear();
        tempDirectMemList.setCapacity(capacity);
        tempDirectMemList.zero();
        return tempDirectMemList;
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
                (avgRecordSize != 0 ? avgRecordSize : (avgRecordSize = estimateAvgRecordSize(metadata))));
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
        LogRecord logRecord = LOG.critical().$(message + " [table=").$(tableToken)
                .$(", column=").$safe(getColumnNameSafe(columnIndex))
                .$(", type=").$(ColumnType.nameOf(columnType))
                .$(", long0=").$(long0)
                .$(", long1=").$(long1)
                .$(", long2=").$(long2)
                .$(", long3=").$(long3);
        if (e instanceof CairoException) {
            o3oomObserved = ((CairoException) e).isOutOfMemory();
            lastErrno = lastErrno == 0 ? ((CairoException) e).errno : lastErrno;
            logRecord
                    .$(", msg=").$safe(((CairoException) e).getFlyweightMessage())
                    .$(", errno=").$(lastErrno)
                    .I$();
        } else {
            lastErrno = O3_ERRNO_FATAL;
            logRecord.$(", ex=").$(e).I$();
        }
    }

    private void handleHousekeepingException(Throwable e) {
        // Log the exception stack.
        // In some cases it is possible to leave corrupted state of table writer behind, for example in
        // the case where we discovered we need to distress the writer - exception occurring trying to re-open
        // last partition can leave some columns in "closed" state, with "fd=-1" but that does not stop O3 logic
        // processing -1 as a correct fd leading to havoc.
        distressed = true;
        LOG.error().$("data has been persisted, but we could not perform housekeeping [table=").$(tableToken)
                .$(", error=").$(e)
                .I$();
        CairoException ex;
        if (e instanceof Sinkable) {
            ex = CairoException.nonCritical().put("Data has been persisted, but we could not perform housekeeping [ex=").put((Sinkable) e).put(']');
        } else {
            ex = CairoException.nonCritical().put("Data has been persisted, but we could not perform housekeeping [ex=").put(e.getMessage()).put(']');
        }
        ex.setHousekeeping(true);
        throw ex;
    }

    private void hardLinkAndPurgeColumnFiles(
            String columnName,
            int columnIndex,
            boolean isIndexed,
            CharSequence newName,
            int columnType
    ) {
        try {
            PurgingOperator purgingOperator = getPurgingOperator();
            long newColumnNameTxn = getTxn();
            if (PartitionBy.isPartitioned(partitionBy)) {
                for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
                    // Link files in each partition.
                    long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
                    long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                    long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                    hardLinkAndPurgeColumnFiles(columnName, columnIndex, columnType, isIndexed, newName, partitionTimestamp, partitionNameTxn, newColumnNameTxn, columnNameTxn);
                    if (columnVersionWriter.getRecordIndex(partitionTimestamp, columnIndex) > -1L) {
                        long columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, columnIndex);
                        columnVersionWriter.upsert(partitionTimestamp, columnIndex, newColumnNameTxn, columnTop);
                    }
                }
            } else {
                long columnNameTxn = columnVersionWriter.getColumnNameTxn(txWriter.getLastPartitionTimestamp(), columnIndex);
                hardLinkAndPurgeColumnFiles(columnName, columnIndex, columnType, isIndexed, newName, txWriter.getLastPartitionTimestamp(), -1L, newColumnNameTxn, columnNameTxn);
                long columnTop = columnVersionWriter.getColumnTop(txWriter.getLastPartitionTimestamp(), columnIndex);
                columnVersionWriter.upsert(txWriter.getLastPartitionTimestamp(), columnIndex, newColumnNameTxn, columnTop);
            }

            if (ColumnType.isSymbol(columnType)) {
                // Link .o, .c, .k, .v symbol files in the table root folder
                long symbolTableNameTxn = columnVersionWriter.getSymbolTableNameTxn(columnIndex);
                try {
                    linkFile(
                            ff,
                            charFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn),
                            charFileName(other.trimTo(pathSize), newName, newColumnNameTxn)
                    );
                    linkFile(
                            ff,
                            offsetFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn),
                            offsetFileName(other.trimTo(pathSize), newName, newColumnNameTxn)
                    );
                    linkFile(
                            ff,
                            keyFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn),
                            keyFileName(other.trimTo(pathSize), newName, newColumnNameTxn)
                    );
                    linkFile(
                            ff,
                            valueFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn),
                            valueFileName(other.trimTo(pathSize), newName, newColumnNameTxn)
                    );
                } catch (Throwable e) {
                    ff.removeQuiet(offsetFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                    ff.removeQuiet(charFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                    ff.removeQuiet(keyFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                    ff.removeQuiet(valueFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                    throw e;
                }
                purgingOperator.add(columnIndex, columnName, columnType, isIndexed, symbolTableNameTxn, PurgingOperator.TABLE_ROOT_PARTITION, -1L);
                columnVersionWriter.upsertSymbolTableTxnName(columnIndex, newColumnNameTxn);
            }
            final long columnAddedPartition = columnVersionWriter.getColumnTopPartitionTimestamp(columnIndex);
            columnVersionWriter.upsertDefaultTxnName(columnIndex, newColumnNameTxn, columnAddedPartition);
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    private void hardLinkAndPurgeColumnFiles(String columnName, int columnIndex, int columnType, boolean isIndexed, CharSequence newName, long partitionTimestamp, long partitionNameTxn, long newColumnNameTxn, long columnNameTxn) {
        setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        setPathForNativePartition(other, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        int plen = path.size();
        linkFile(ff, dFile(path.trimTo(plen), columnName, columnNameTxn), dFile(other.trimTo(plen), newName, newColumnNameTxn));
        if (ColumnType.isVarSize(columnType)) {
            linkFile(ff, iFile(path.trimTo(plen), columnName, columnNameTxn), iFile(other.trimTo(plen), newName, newColumnNameTxn));
        } else if (ColumnType.isSymbol(columnType) && isIndexed) {
            linkFile(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn), keyFileName(other.trimTo(plen), newName, newColumnNameTxn));
            linkFile(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn), valueFileName(other.trimTo(plen), newName, newColumnNameTxn));
        }
        path.trimTo(pathSize);
        other.trimTo(pathSize);
        purgingOperator.add(columnIndex, columnName, columnType, isIndexed, columnNameTxn, partitionTimestamp, partitionNameTxn);
    }

    private void hardLinkAndPurgeSymbolTableFiles(
            String columnName,
            int columnIndex,
            boolean isIndexed,
            CharSequence newName
    ) {
        try {
            PurgingOperator purgingOperator = getPurgingOperator();
            long newColumnNameTxn = getTxn();
            long symbolTableNameTxn = columnVersionWriter.getSymbolTableNameTxn(columnIndex);
            // Link .o, .c, .k, .v symbol files in the table root folder
            try {
                linkFile(
                        ff,
                        charFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn),
                        charFileName(other.trimTo(pathSize), newName, newColumnNameTxn)
                );
                // in case it's symbol capacity rebuild copy symbol offset file.
                // it's almost the same but the capacity in the file header is changed
                offsetFileName(other.trimTo(pathSize), newName, newColumnNameTxn);
                if (Os.isWindows() && ff.exists(other.$())) {
                    ff.remove(other.$());
                }
                if (ff.copy(offsetFileName(path.trimTo(pathSize), columnName, symbolTableNameTxn), other.$()) < 0) {
                    throw CairoException.critical(ff.errno())
                            .put("Could not copy [from=").put(path)
                            .put(", to=").put(other)
                            .put(']');

                }
            } catch (Throwable e) {
                ff.removeQuiet(offsetFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                ff.removeQuiet(charFileName(other.trimTo(pathSize), newName, newColumnNameTxn));
                throw e;
            }

            purgingOperator.add(columnIndex, columnName, ColumnType.SYMBOL, isIndexed, symbolTableNameTxn, PurgingOperator.TABLE_ROOT_PARTITION, -1L);
            columnVersionWriter.upsertSymbolTableTxnName(columnIndex, newColumnNameTxn);
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    /**
     * House keeps table after commit. The tricky bit is to run this housekeeping on each commit. Commit() itself
     * has a contract that if exception is thrown, the data is not committed. However, if this housekeeping fails,
     * the data IS committed.
     * <p>
     * What we need to achieve, is to report that data has been committed, but table writes should be discarded. It is
     * necessary when housekeeping runs into an error. To indicate the situation where data is committed.
     */
    private void housekeep() {
        try {
            squashSplitPartitions(minSplitPartitionTimestamp, txWriter.getMaxTimestamp(), configuration.getO3LastPartitionMaxSplits());
            processPartitionRemoveCandidates();
            metrics.tableWriterMetrics().incrementCommits();
            enforceTtl();
            scaleSymbolCapacities();
        } catch (Throwable e) {
            // Log the exception stack.
            handleHousekeepingException(e);
        }
    }

    private void indexHistoricPartitions(SymbolColumnIndexer indexer, CharSequence columnName, int indexValueBlockSize, int columnIndex) {
        long ts = txWriter.getMaxTimestamp();
        if (ts > Numbers.LONG_NULL) {
            try {
                // Index last partition separately
                for (int i = 0, n = txWriter.getPartitionCount() - 1; i < n; i++) {
                    long timestamp = txWriter.getPartitionTimestampByIndex(i);
                    path.trimTo(pathSize);
                    setStateForTimestamp(path, timestamp);

                    if (ff.exists(path.$())) {
                        final int plen = path.size();
                        final long columnNameTxn = columnVersionWriter.getColumnNameTxn(timestamp, columnIndex);
                        if (txWriter.isPartitionParquet(i)) {
                            indexParquetPartition(indexer, columnName, i, columnIndex, columnNameTxn, indexValueBlockSize, plen, timestamp);
                        } else if (ff.exists(dFile(path.trimTo(plen), columnName, columnNameTxn))) {
                            indexNativePartition(indexer, columnName, columnIndex, columnNameTxn, indexValueBlockSize, plen, timestamp);
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

    private void indexNativePartition(
            SymbolColumnIndexer indexer,
            CharSequence columnName,
            int columnIndex,
            long columnNameTxn,
            int indexValueBlockSize,
            int plen,
            long timestamp
    ) {
        path.trimTo(plen);
        LOG.info().$("indexing [path=").$substr(pathRootSize, path).I$();

        createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);
        final long partitionSize = txWriter.getPartitionRowCountByTimestamp(timestamp);
        final long columnTop = columnVersionWriter.getColumnTop(timestamp, columnIndex);

        if (columnTop > -1 && partitionSize > columnTop) {
            long columnDataFd = openRO(ff, dFile(path.trimTo(plen), columnName, columnNameTxn), LOG);
            try {
                indexer.configureWriter(path.trimTo(plen), columnName, columnNameTxn, columnTop);
                indexer.index(ff, columnDataFd, columnTop, partitionSize);
            } finally {
                ff.close(columnDataFd);
            }
        }
    }

    private void indexParquetPartition(
            SymbolColumnIndexer indexer,
            CharSequence columnName,
            int partitionIndex,
            int columnIndex,
            long columnNameTxn,
            int indexValueBlockSize,
            int plen,
            long timestamp
    ) {
        // parquet partition
        path.trimTo(plen);
        LOG.info().$("indexing parquet [path=").$substr(pathRootSize, path).I$();

        long parquetAddr = 0;
        long parquetSize = 0;
        try (RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_TABLE_WRITER)) {
            parquetSize = txWriter.getPartitionParquetFileSize(partitionIndex);
            parquetAddr = mapRO(ff, path.concat(PARQUET_PARTITION_NAME).$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            parquetDecoder.of(parquetAddr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            final PartitionDecoder.Metadata parquetMetadata = parquetDecoder.metadata();

            int parquetColumnIndex = -1;
            for (int idx = 0, cnt = parquetMetadata.getColumnCount(); idx < cnt; idx++) {
                if (parquetMetadata.getColumnId(idx) == columnIndex) {
                    parquetColumnIndex = idx;
                    break;
                }
            }
            if (parquetColumnIndex == -1) {
                path.trimTo(plen);
                LOG.error().$("could not find symbol column for indexing in parquet, skipping [path=").$substr(pathRootSize, path)
                        .$(", columnIndex=").$(columnIndex)
                        .I$();
                return;
            }

            createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);
            final long partitionSize = txWriter.getPartitionRowCountByTimestamp(timestamp);
            final long columnTop = columnVersionWriter.getColumnTop(timestamp, columnIndex);

            if (columnTop > -1 && partitionSize > columnTop) {
                indexer.configureWriter(path.trimTo(plen), columnName, columnNameTxn, columnTop);

                parquetColumnIdsAndTypes.clear();
                parquetColumnIdsAndTypes.add(parquetColumnIndex);
                parquetColumnIdsAndTypes.add(ColumnType.SYMBOL);

                long rowCount = 0;
                final int rowGroupCount = parquetMetadata.getRowGroupCount();
                final BitmapIndexWriter indexWriter = indexer.getWriter();
                for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
                    final int rowGroupSize = parquetMetadata.getRowGroupSize(rowGroupIndex);
                    if (rowCount + rowGroupSize <= columnTop) {
                        rowCount += rowGroupSize;
                        continue;
                    }

                    parquetDecoder.decodeRowGroup(
                            rowGroupBuffers,
                            parquetColumnIdsAndTypes,
                            rowGroupIndex,
                            (int) Math.max(0, columnTop - rowCount),
                            rowGroupSize
                    );

                    long rowId = Math.max(rowCount, columnTop);
                    final long addr = rowGroupBuffers.getChunkDataPtr(0);
                    final long size = rowGroupBuffers.getChunkDataSize(0);
                    for (long p = addr, lim = addr + size; p < lim; p += 4, rowId++) {
                        indexWriter.add(TableUtils.toIndexKey(Unsafe.getUnsafe().getInt(p)), rowId);
                    }

                    rowCount += rowGroupSize;
                }
                indexWriter.setMaxValue(partitionSize - 1);
            }
        } finally {
            if (parquetAddr != 0) {
                ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            Misc.free(parquetDecoder);
        }
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

        // column could not provide the necessary length of buffer
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

    private Row newRowO3(long timestamp) {
        LOG.info().$("switched to o3 [table=").$(tableToken).I$();
        txWriter.beginPartitionSizeUpdate();
        o3OpenColumns();
        o3InError = false;
        o3MasterRef = masterRef;
        rowAction = ROW_ACTION_O3;
        o3TimestampSetter(timestamp);
        return row;
    }

    /**
     * Commits O3 data. Lag is optional. When 0 is specified, the entire O3 segment is committed.
     *
     * @param o3MaxLag interval in microseconds that determines the length of O3 segment that is not going to be
     *                 committed to disk. The interval starts at max timestamp of the O3 segment and ends <i>o3MaxLag</i>
     *                 microseconds before this timestamp.
     * @return <i>true</i> when commit has is a NOOP, e.g., no data has been committed to disk. <i>false</i> otherwise.
     */
    private boolean o3Commit(long o3MaxLag) {
        o3RowCount = getO3RowCount0();

        long o3LagRowCount = 0;
        long maxUncommittedRows = metadata.getMaxUncommittedRows();
        final int timestampColumnIndex = metadata.getTimestampIndex();
        lastPartitionTimestamp = txWriter.getPartitionTimestampByTimestamp(partitionTimestampHi);
        // we will check new partitionTimestampHi value against the limit to see if the writer
        // will have to switch partition internally
        long partitionTimestampHiLimit = txWriter.getCurrentPartitionMaxTimestamp(partitionTimestampHi);
        try {
            o3RowCount += o3MoveUncommitted();

            // we may need to re-use file descriptors when this partition is the "current" one
            // we cannot open file again due to sharing violation
            //
            // to determine that 'ooTimestampLo' goes into the current partition
            // we need to compare 'partitionTimestampHi', which is appropriately truncated to DAY/MONTH/YEAR
            // to this.maxTimestamp, which isn't truncated yet. So we need to truncate it first
            LOG.debug().$("sorting o3 [table=").$(tableToken).I$();
            final long sortedTimestampsAddr = o3TimestampMem.getAddress();

            // resize timestamp memory if needed
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
            if (o3TimestampMin < TIMESTAMP_EPOCH) {
                o3InError = true;
                throw CairoException.nonCritical().put("O3 commit encountered timestamp before 1970-01-01");
            }

            long o3TimestampMax = getTimestampIndexValue(sortedTimestampsAddr, o3RowCount - 1);
            if (o3TimestampMax < TIMESTAMP_EPOCH) {
                o3InError = true;
                throw CairoException.nonCritical().put("O3 commit encountered timestamp before 1970-01-01");
            }

            // Safe check of the sort. No known way to reproduce
            assert o3TimestampMin <= o3TimestampMax;

            if (o3MaxLag > 0) {
                long lagError = 0;
                // convert o3MaxLag from microseconds to timestamps
                o3MaxLag = timestampDriver.fromMicros(o3MaxLag);
                if (getMaxTimestamp() != Long.MIN_VALUE) {
                    // When table already has data we can calculate the overlap of the newly added
                    // batch of records with existing data in the table. The positive value of the overlap
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
                            Vect.BIN_SEARCH_SCAN_DOWN
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

                LOG.info().$("o3 commit [table=").$(tableToken)
                        .$(", maxUncommittedRows=").$(maxUncommittedRows)
                        .$(", o3TimestampMin=").$ts(timestampDriver, o3TimestampMin)
                        .$(", o3TimestampMax=").$ts(timestampDriver, o3TimestampMax)
                        .$(", o3MaxLag=").$(o3MaxLag)
                        .$(", o3EffectiveLag=").$(o3EffectiveLag)
                        .$(", lagError=").$(lagError)
                        .$(", o3SpreadUs=").$(o3TimestampMax - o3TimestampMin)
                        .$(", lagThresholdTimestamp=").$ts(timestampDriver, lagThresholdTimestamp)
                        .$(", o3LagRowCount=").$(o3LagRowCount)
                        .$(", srcOooMax=").$(srcOooMax)
                        .$(", o3RowCount=").$(o3RowCount)
                        .I$();

            } else {
                LOG.info()
                        .$("o3 commit [table=").$(tableToken)
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
                    .$("sorted [table=").$(tableToken)
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
                    TableWriterPressureControl.EMPTY
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
            boolean isParquet,
            long o3TimestampLo,
            long o3TimestampHi
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
                    isParquet,
                    o3TimestampLo,
                    o3TimestampHi
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
                    isParquet,
                    o3TimestampLo,
                    o3TimestampHi
            );
        }
    }

    private void o3ConsumePartitionUpdateSink() {
        long blockIndex = -1;

        long commitTransientRowCount = txWriter.transientRowCount;
        boolean partitionsRemoved = false, firstPartitionRemoved = false, lastPartitionRemoved = false;

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
                if (!partitionMutates && srcDataNewPartitionSize < 0) {
                    // noop
                    continue;
                }

                boolean isMinPartitionUpdate = partitionTimestamp == txWriter.getPartitionTimestampByTimestamp(txWriter.getMinTimestamp())
                        && partitionTimestamp == txWriter.getPartitionTimestampByTimestamp(timestampMin);
                boolean isFirstPartitionReplaced = isCommitReplaceMode() && isMinPartitionUpdate;

                txWriter.minTimestamp = isFirstPartitionReplaced ? timestampMin : Math.min(timestampMin, txWriter.minTimestamp);
                int partitionIndexRaw = txWriter.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp);

                final long newPartitionTimestamp = partitionTimestamp;
                final int newPartitionIndex = partitionIndexRaw;
                if (partitionIndexRaw < 0) {
                    // This is partition split. Instead of rewriting partition because of O3 merge,
                    // the partition is kept, and its tail rewritten.
                    // The new partition overlaps in time with the previous one.
                    partitionTimestamp = txWriter.getPartitionTimestampByTimestamp(partitionTimestamp);
                    partitionIndexRaw = txWriter.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp);
                }

                if (isCommitReplaceMode() && srcDataOldPartitionSize > 0 && srcDataNewPartitionSize < srcDataOldPartitionSize) {
                    if (!partitionMutates) {
                        // Replace resulted in trimming the partition.
                        // Now trim the column tops so that they don't exceed the partition size
                        o3ConsumePartitionUpdateSink_trimPartitionColumnTops(partitionTimestamp, srcDataNewPartitionSize);
                    }

                    if (partitionTimestamp == lastPartitionTimestamp) {
                        // Recalculate max timestamp
                        partitionsRemoved = true;
                        lastPartitionRemoved = true;
                    }
                }

                if (partitionTimestamp == lastPartitionTimestamp && newPartitionTimestamp == partitionTimestamp) {
                    if (partitionMutates) {
                        // The last partition is rewritten.
                        closeActivePartition(true);
                    } else if (!isLastWrittenPartition) {
                        // The last partition is appended, and it is not the last partition anymore.
                        closeActivePartition(srcDataNewPartitionSize);
                    } else {
                        // The last partition is appended, and it is still the last partition.
                        setAppendPosition(srcDataNewPartitionSize, false);
                    }
                }

                if (partitionTimestamp < lastPartitionTimestamp) {
                    // increment fixedRowCount by number of rows old partition incremented
                    txWriter.fixedRowCount += srcDataNewPartitionSize - srcDataOldPartitionSize + o3SplitPartitionSize;
                } else {
                    if (partitionTimestamp != lastPartitionTimestamp) {
                        txWriter.fixedRowCount += commitTransientRowCount;
                    }
                    if (o3SplitPartitionSize > 0) {
                        // yep, it was
                        // the "current" active becomes fixed
                        txWriter.fixedRowCount += srcDataNewPartitionSize;
                        commitTransientRowCount = o3SplitPartitionSize;
                    } else {
                        commitTransientRowCount = srcDataNewPartitionSize;
                    }
                }

                LOG.info().$("o3 partition update [timestampMin=").$ts(timestampDriver, timestampMin)
                        .$(", last=").$(partitionTimestamp == lastPartitionTimestamp)
                        .$(", partitionTimestamp=").$ts(timestampDriver, partitionTimestamp)
                        .$(", partitionMutates=").$(partitionMutates)
                        .$(", lastPartitionTimestamp=").$ts(timestampDriver, lastPartitionTimestamp)
                        .$(", srcDataOldPartitionSize=").$(srcDataOldPartitionSize)
                        .$(", srcDataNewPartitionSize=").$(srcDataNewPartitionSize)
                        .$(", o3SplitPartitionSize=").$(o3SplitPartitionSize)
                        .$(", commitTransientRowCount=").$(commitTransientRowCount)
                        .$(", fixedRowCount=").$(this.txWriter.fixedRowCount)
                        .I$();

                if (newPartitionTimestamp != partitionTimestamp) {
                    LOG.info()
                            .$("o3 split partition [table=").$(tableToken)
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
                        // Close the last partition without truncating it.
                        long committedLastPartitionSize = txWriter.getPartitionRowCountByTimestamp(partitionTimestamp);
                        closeActivePartition(committedLastPartitionSize);
                    }
                }

                if (partitionMutates && newPartitionTimestamp == partitionTimestamp) {
                    final long srcNameTxn = txWriter.getPartitionNameTxnByRawIndex(partitionIndexRaw);
                    LOG.info()
                            .$("merged partition [table=").$(tableToken)
                            .$(", ts=").$ts(timestampDriver, partitionTimestamp)
                            .$(", txn=").$(txWriter.txn)
                            .$(", rows=").$(srcDataNewPartitionSize)
                            .I$();

                    if (isCommitReplaceMode() && srcDataNewPartitionSize == 0) {
                        // Partition data is fully removed by the replace-commit
                        int partIndex = txWriter.getPartitionIndex(partitionTimestamp);
                        boolean isSplitPartition = partIndex > 0
                                && txWriter.getPartitionFloor(txWriter.getPartitionTimestampByIndex(partIndex - 1)) == txWriter.getPartitionFloor(partitionTimestamp);

                        // It is not easy to remove the split
                        // the parent can become writable, and we can overwrite / truncate data visible to the readers
                        if (!isSplitPartition) {
                            LOG.info().$("partition is fully removed in range replace [table=").$(tableToken)
                                    .$(", ts=").$ts(timestampDriver, partitionTimestamp)
                                    .I$();

                            txWriter.removeAttachedPartitions(partitionTimestamp);
                            columnVersionWriter.removePartition(partitionTimestamp);
                            partitionRemoveCandidates.add(partitionTimestamp, srcNameTxn);
                        } else {
                            // Set partition size to 0 and process all 0 size partitions at the end of the method.
                            // It will be removed if there are no readers on the previous partition.
                            txWriter.updatePartitionSizeByTimestamp(partitionTimestamp, srcDataNewPartitionSize);
                        }

                        partitionsRemoved = true;
                        firstPartitionRemoved |= partIndex == 0;
                        boolean removedIsLast = partIndex == txWriter.getPartitionCount();
                        lastPartitionRemoved |= removedIsLast;

                        if (removedIsLast) {
                            if (partIndex > 0) {
                                int newLastPartitionIndex = partIndex - 1;
                                long newLastPartitionTimestamp = txWriter.getPartitionTimestampByIndex(newLastPartitionIndex);
                                long newLastPartitionSize = txWriter.getPartitionSize(newLastPartitionIndex);
                                columnVersionWriter.replaceInitialPartitionRecords(newLastPartitionTimestamp, newLastPartitionSize);

                                // If a split partition is removed, it may leave the previous partition
                                // with column top sticking out of the partition size.
                                // This "sticking out" is not handled if it is the last partition.
                                o3ConsumePartitionUpdateSink_trimPartitionColumnTops(newLastPartitionTimestamp, newLastPartitionSize);
                            } else {
                                // All partitions are removed
                                columnVersionWriter.truncate();
                            }
                        }
                    } else {
                        final long parquetFileSize = Unsafe.getUnsafe().getLong(blockAddress + 7 * Long.BYTES);
                        if (parquetFileSize > -1) {
                            txWriter.updatePartitionSizeByRawIndex(partitionIndexRaw, partitionTimestamp, srcDataNewPartitionSize);
                            txWriter.setPartitionParquetFormat(partitionTimestamp, parquetFileSize);
                        } else {
                            txWriter.updatePartitionSizeAndTxnByRawIndex(partitionIndexRaw, srcDataNewPartitionSize);
                            partitionRemoveCandidates.add(partitionTimestamp, srcNameTxn);
                        }
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
        if (partitionsRemoved) {
            o3ConsumePartitionUpdateSink_processSplitPartitionRemoval();

            // Replace commit removed some of the partitions
            if (txWriter.getPartitionCount() > 0) {
                try {
                    if (firstPartitionRemoved) {
                        // First partition is removed and no data added in the replace commit
                        // We need to read the min timestamp from the next partition
                        long firstPartitionTimestamp = txWriter.getPartitionTimestampByIndex(0);
                        long partitionSize = txWriter.getPartitionSize(0);
                        setPathForNativePartition(path, timestampType, partitionBy, firstPartitionTimestamp, txWriter.getPartitionNameTxn(0));
                        readPartitionMinMaxTimestamps(firstPartitionTimestamp, path, metadata.getColumnName(metadata.getTimestampIndex()), -1, partitionSize);
                        txWriter.minTimestamp = attachMinTimestamp;
                    }

                    if (lastPartitionRemoved) {
                        int lastPartitionIndex = txWriter.getPartitionCount() - 1;
                        long lastPartitionTimestamp = txWriter.getPartitionTimestampByIndex(lastPartitionIndex);
                        long partitionSize = txWriter.getPartitionSize(lastPartitionIndex);
                        setPathForNativePartition(path.trimTo(pathSize), timestampType, partitionBy, lastPartitionTimestamp, txWriter.getPartitionNameTxn(lastPartitionIndex));
                        readPartitionMinMaxTimestamps(lastPartitionTimestamp, path, metadata.getColumnName(metadata.getTimestampIndex()), -1, partitionSize);
                        txWriter.maxTimestamp = attachMaxTimestamp;
                    }
                } finally {
                    path.trimTo(pathSize);
                }

                txWriter.finishPartitionSizeUpdate(txWriter.getMinTimestamp(), txWriter.getMaxTimestamp());
                assert partitionTimestampHi != Long.MIN_VALUE;
            } else {
                LOG.info().$("replace commit truncated the table [table=").$(tableToken).$();
                // All partitions are removed. The table is in the same state as softly truncated
                freeColumns(false);
                releaseIndexerWriters();
                partitionTimestampHi = Long.MIN_VALUE;
                lastPartitionTimestamp = Long.MIN_VALUE;

                rowAction = ROW_ACTION_OPEN_PARTITION;
                txWriter.resetTimestamp();

                columnVersionWriter.truncate();
                txWriter.truncate(columnVersionWriter.getVersion(), denseSymbolMapWriters);
            }
            txWriter.bumpPartitionTableVersion();
        } else {
            txWriter.transientRowCount = commitTransientRowCount;
        }
    }

    private void o3ConsumePartitionUpdateSink_findNewSplitPartitionSizeTimestamp(Path partitionPath, long partitionTimestamp, long partitionSize) {
        // Find how many rows of the same timestamp we need to split out to create a minimum size split partition
        // this is needed sometimes when split partition is fully removed in a replace commit
        int pathSize = partitionPath.size();
        try {
            CharSequence tsColumnName = metadata.getColumnName(metadata.getTimestampIndex());
            final long fd = openRO(ff, dFile(partitionPath, tsColumnName, COLUMN_NAME_TXN_NONE), LOG);
            try {
                final long mapSize = partitionSize * Long.BYTES;
                final long addr = mapRO(ff, fd, mapSize, MemoryTag.MMAP_TABLE_WRITER);
                try {
                    long lastTs = Unsafe.getUnsafe().getLong(addr + (partitionSize - 1) * Long.BYTES);

                    long currentTs = lastTs - 1;
                    long row = partitionSize - 2;
                    for (; row >= 0; row--) {
                        currentTs = Unsafe.getUnsafe().getLong(addr + row * Long.BYTES);
                        if (currentTs != lastTs) {
                            break;
                        }
                    }
                    // Use attachMinTimestamp field to pass new partition size
                    attachMinTimestamp = row + 1;
                    // Use attachMaxTimestamp field to pass new partition max timestamp
                    attachMaxTimestamp = row > -1 ? currentTs + 1 : partitionTimestamp;
                } finally {
                    ff.munmap(addr, mapSize, MemoryTag.MMAP_TABLE_WRITER);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            partitionPath.trimTo(pathSize);
        }
    }

    private void o3ConsumePartitionUpdateSink_processSplitPartitionRemoval() {
        // Process all the partitions with 0 sizes
        // after running o3ConsumePartitionUpdateSink()
        // 0 size partition means that the partition cannot be removed
        // straight away because it is
        // - split partition, say 2024-02-24T1258
        // - parent partition 2024-02-24 may be used by a reader locked on txn before the split
        //   and cannot have any data to be appended or partition files to be truncated
        // The solution to this situation is before removal of the split partition 2024-02-24T1258
        // we split one more partition from the parent partition 2024-02-24
        // with 1 line (or minimum number of lines with the same timestamp)
        // to another split, for example 2024-02-24T1257
        // and then drop the split partition 2024-02-24T1258
        for (int i = txWriter.getPartitionCount() - 1; i > 0; i--) {
            long partitionSize = txWriter.getPartitionSize(i);
            if (partitionSize == 0) {
                // This is a split partition that is fully removed in the last commit.
                long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
                long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                long prevPartitionTimestamp = txWriter.getPartitionTimestampByIndex(i - 1);
                long prevPartitionSize = txWriter.getPartitionSize(i - 1);
                long prevPartitionTxn = txWriter.getPartitionNameTxn(i - 1);

                if (txWriter.getPartitionFloor(partitionTimestamp) != txWriter.getPartitionFloor(prevPartitionTimestamp)
                        || prevPartitionSize == 0
                        || prevPartitionTxn == txWriter.txn
                ) {
                    // Previous partition is not the parent partition of this removed split
                    // or previous partition is also 0 size, e.g. removed split
                    // or the previous partition was just re-created in this commit
                    // in this case we can remove the current partition fully
                    partitionRemoveCandidates.add(partitionTimestamp, partitionNameTxn);
                    txWriter.removeAttachedPartitions(partitionTimestamp);
                } else {
                    try {
                        // The safe way to remove the split is to split one line from the parent partition
                        // See comment in the beginning of this method.
                        long prevPartitionNameTxn = setStateForTimestamp(path, prevPartitionTimestamp);
                        o3ConsumePartitionUpdateSink_findNewSplitPartitionSizeTimestamp(
                                path,
                                prevPartitionTimestamp,
                                prevPartitionSize
                        );
                        long newPrevPartitionSize = attachMinTimestamp;
                        long newSplitPartitionTimestamp = attachMaxTimestamp;

                        assert newSplitPartitionTimestamp < partitionTimestamp || (newPrevPartitionSize == 0 && newSplitPartitionTimestamp == partitionTimestamp);

                        // This logging should be quite rare, we can afford info level.
                        LOG.info().$("splitting last line of the partition [table=").$(tableToken)
                                .$(", partition=").$ts(prevPartitionTimestamp)
                                .$(", oldSize=").$(prevPartitionSize)
                                .$(", newSize=").$(newPrevPartitionSize)
                                .$(", splitPartition=").$ts(timestampDriver, newSplitPartitionTimestamp)
                                .$(", splitPartitionNameTxn=").$(txWriter.txn)
                                .$(", deletedSplitPartition=").$ts(timestampDriver, partitionTimestamp)
                                .$();

                        int insertPartitionIndex = i;
                        FrameFactory frameFactory = engine.getFrameFactory();
                        try (Frame sourceFrame = frameFactory.openRO(path, prevPartitionTimestamp, metadata, columnVersionWriter, prevPartitionSize)) {
                            // Create the source frame and then manipulate partitions in txWriter
                            // When newSplitPartitionTimestamp == partitionTimestamp it is the only way
                            // to open 2 frames to the same partition timestamp
                            if (newPrevPartitionSize == 0) {
                                // newSplitPartitionTimestamp can be equal to partitionTimestamp
                                partitionRemoveCandidates.add(prevPartitionTimestamp, prevPartitionNameTxn);
                                insertPartitionIndex = txWriter.removeAttachedPartitions(prevPartitionTimestamp);
                            } else {
                                txWriter.updatePartitionSizeByTimestamp(prevPartitionTimestamp, newPrevPartitionSize);
                            }

                            txWriter.insertPartition(insertPartitionIndex, newSplitPartitionTimestamp, prevPartitionSize - newPrevPartitionSize, txWriter.txn);
                            setStateForTimestamp(other, newSplitPartitionTimestamp);
                            ff.mkdir(other.$(), configuration.getMkDirMode());
                            try (Frame targetFrame = frameFactory.createRW(other, newSplitPartitionTimestamp, metadata, columnVersionWriter, 0)) {
                                FrameAlgebra.append(targetFrame, sourceFrame, newPrevPartitionSize, prevPartitionSize, configuration.getCommitMode());
                            }
                        }
                        addPhysicallyWrittenRows(prevPartitionSize - newPrevPartitionSize);

                        // Now it's safe to remove the empty split partition
                        partitionRemoveCandidates.add(partitionTimestamp, partitionNameTxn);
                        txWriter.removeAttachedPartitions(partitionTimestamp);
                    } finally {
                        path.trimTo(pathSize);
                        other.trimTo(pathSize);
                    }
                }
            }
        }
    }

    private void o3ConsumePartitionUpdateSink_trimPartitionColumnTops(long partitionTimestamp, long newPartitionSize) {
        int columnCount = metadata.getColumnCount();
        for (int column = 0; column < columnCount; column++) {
            long colTop = getColumnTop(partitionTimestamp, column, -1);
            if (colTop > newPartitionSize) {
                columnVersionWriter.upsertColumnTop(partitionTimestamp, column, newPartitionSize);
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
                    O3CopyJob.unmapAndClose(
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
                    .$("o3 move uncommitted [table=").$(tableToken)
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
            if (o3IndexMem == null) {
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
        openPartition(ts, txWriter.getTransientRowCount() + txWriter.getLagRowCount());
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

            // index must be created before a column is initialised because
            // it uses a primary column object as a temporary tool
            if (indexFlag) {
                createIndexFiles(name, columnNameTxn, indexValueBlockCapacity, plen, true);
            }

            openColumnFiles(name, columnNameTxn, columnIndex, plen);
            if (txWriter.getTransientRowCount() > 0) {
                // write top offset to the column version file
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

            LOG.info().$("ADDED column '").$safe(name)
                    .$('[').$(ColumnType.nameOf(columnType)).$("], columnName txn ").$(columnNameTxn)
                    .$(" to ").$substr(pathRootSize, path)
                    .$(" with columnTop ").$(txWriter.getTransientRowCount())
                    .$();
        } finally {
            path.trimTo(pathSize);
        }
    }

    private void openPartition(long timestamp, long rowCount) {
        try {
            timestamp = txWriter.getPartitionTimestampByTimestamp(timestamp);
            lastOpenPartitionTxnName = setStateForTimestamp(path, timestamp);
            partitionTimestampHi = txWriter.getCurrentPartitionMaxTimestamp(timestamp);
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
                        // because we are reusing MAMemoryImpl object from columns' list
                        createIndexFiles(name, columnNameTxn, metadata.getIndexValueBlockCapacity(i), plen, rowCount < 1);
                    }

                    openColumnFiles(name, columnNameTxn, i, plen);

                    if (indexer != null) {
                        final long columnTop = columnVersionWriter.getColumnTopQuick(lastOpenPartitionTs, i);
                        indexer.configureFollowerAndWriter(path, name, columnNameTxn, getPrimaryColumn(i), columnTop);
                    }
                }
            }
            populateDenseIndexerList();

            LOG.info().$("switched partition [path=").$substr(pathRootSize, path)
                    .$(", rowCount=").$(rowCount)
                    .I$();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        } finally {
            path.trimTo(pathSize);
        }
    }

    private long openTodoMem(long tableTxn) {
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
                // also check that the _todo_ file belongs to the latest transaction in _txn, otherwise ignore it
                if (todoTxn != tableTxn || todoMem.getLong(24) != todoTxn) {
                    todoMem.putLong(8, configuration.getDatabaseIdLo());
                    todoMem.putLong(16, configuration.getDatabaseIdHi());
                    Unsafe.getUnsafe().storeFence();
                    todoMem.putLong(24, todoTxn);
                    return 0;
                }

                return todoMem.getLong(32);
            } else {
                resetTodoLog(ff, path, pathSize, todoMem);
                todoTxn = 0;
                return 0;
            }
        } finally {
            path.trimTo(pathSize);
        }
    }

    private void performRecovery() {
        rollbackIndexes();
        rollbackSymbolTables(false);
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
                    .$(", table=").$(tableToken)
                    .$(", tableId=").$(tableId)
                    .$(", correlationId=").$(correlationId)
                    .$(", cursor=").$(cursor)
                    .I$();
            asyncWriterCommand = asyncWriterCommand.deserialize(cmd);
            affectedRowsCount = asyncWriterCommand.apply(this, contextAllowsAnyStructureChanges);
        } catch (TableReferenceOutOfDateException ex) {
            LOG.info()
                    .$("cannot complete async cmd, reader is out of date [type=").$(cmdType)
                    .$(", table=").$(tableToken)
                    .$(", tableId=").$(tableId)
                    .$(", correlationId=").$(correlationId)
                    .I$();
            errorCode = READER_OUT_OF_DATE;
            errorMsg = ex.getMessage();
        } catch (AlterTableContextException ex) {
            LOG.info()
                    .$("cannot complete async cmd, table structure change is not allowed [type=").$(cmdType)
                    .$(", tableName=").$(tableToken)
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
                    .$(", tableName=").$(tableToken)
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
            final long o3TimestampMin,
            final long o3TimestampMax,
            boolean flattenTimestamp,
            long rowLo,
            TableWriterPressureControl pressureControl
    ) {
        o3ErrorCount.set(0);
        o3oomObserved = false;
        lastErrno = 0;
        partitionRemoveCandidates.clear();
        o3ColumnCounters.clear();
        o3BasketPool.clear();
        commitRowCount = srcOooMax;

        // move uncommitted is liable to change max timestamp,
        // however, we need to identify the last partition before max timestamp skips to NULL, for example
        final long maxTimestamp = txWriter.getMaxTimestamp();
        final long transientRowCount = txWriter.transientRowCount;

        o3DoneLatch.reset();
        o3PartitionUpdRemaining.set(0L);
        boolean success = true;
        int latchCount = 0;
        long srcOoo = rowLo;
        int pCount = 0;
        int partitionParallelism = pressureControl.getMemoryPressureRegulationValue();
        long replaceMaxTimestamp = Long.MIN_VALUE;
        long partitionTimestamp = o3TimestampMin;
        final long minO3PartitionTimestamp = txWriter.getPartitionTimestampByTimestamp(o3TimestampMin);
        long maxO3PartitionTimestamp = txWriter.getPartitionTimestampByTimestamp(o3TimestampMax);

        try {
            resizePartitionUpdateSink();

            // One loop iteration per partition.
            int inflightPartitions = 0;
            while (srcOoo < srcOooMax || (isCommitReplaceMode() && partitionTimestamp <= o3TimestampMax)) {
                pressureControl.updateInflightPartitions(++inflightPartitions);
                try {
                    final long srcOooLo = srcOoo;
                    final long o3Timestamp;
                    if (!isCommitReplaceMode()) {
                        o3Timestamp = getTimestampIndexValue(sortedTimestampsAddr, srcOoo);
                    } else {
                        if (srcOoo < srcOooMax) {
                            // There is o3 data to process
                            long o3ts = getTimestampIndexValue(sortedTimestampsAddr, srcOoo);
                            if (o3ts < partitionTimestamp) {
                                // o3 data is before next existing partition, add partition
                                o3Timestamp = o3ts;
                            } else {
                                long o3PartitionTs = txWriter.getPartitionTimestampByTimestamp(o3ts);
                                if (o3PartitionTs == partitionTimestamp) {
                                    // o3 data is at the same partition as the next partition
                                    o3Timestamp = o3ts;
                                } else {
                                    if (txWriter.isInsideExistingPartition(partitionTimestamp)) {
                                        // o3 data is after the existing partition
                                        // but the partition is inside replace range.
                                        // Process the partition to remove it from the partitions list
                                        o3Timestamp = partitionTimestamp;
                                    } else {
                                        // there is no partition and no O3 data to process
                                        // switch partition to the next one and continue
                                        partitionTimestamp = txWriter.getNextExistingPartitionTimestamp(partitionTimestamp);
                                        continue;
                                    }
                                }
                            }
                        } else {
                            // There is no O3 data for this partition, but it's inside the replacement range
                            // e.g. the partition will be fully or partially deleted
                            if (txWriter.isInsideExistingPartition(partitionTimestamp)) {
                                // o3 data is after the existing partition
                                // but the partition is inside replace range.
                                // Process the partition to remove it from the partitions list
                                o3Timestamp = partitionTimestamp;
                            } else {
                                // there is no partition and no O3 data to process
                                // switch partition to the next one and continue
                                partitionTimestamp = txWriter.getNextExistingPartitionTimestamp(partitionTimestamp);
                                continue;
                            }
                        }
                    }
                    partitionTimestamp = txWriter.getPartitionTimestampByTimestamp(o3Timestamp);

                    // Check that the value is not 0 (or another unreasonable value) because of reading beyond written range.
                    assert o3Timestamp >= o3TimestampMin;

                    final long srcOooHi;
                    // keep ceil inclusive in the interval
                    final long srcOooTimestampCeil = txWriter.getCurrentPartitionMaxTimestamp(o3Timestamp);
                    if (srcOooTimestampCeil < o3TimestampMax) {
                        srcOooHi = Vect.boundedBinarySearchIndexT(
                                sortedTimestampsAddr,
                                srcOooTimestampCeil,
                                srcOooLo,
                                srcOooMax - 1,
                                Vect.BIN_SEARCH_SCAN_DOWN
                        );
                    } else {
                        srcOooHi = srcOooMax - 1;
                    }

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
                        // When partition is created without an O3 merge, use `txn-1` as the partition version.
                        // `txn` version is used when partition is merged. Both `txn-1` and `txn` can
                        // be written within the same commit when new partition is initially written in order
                        // and then O3 triggers a merge of the partition.
                        srcNameTxn = txWriter.getTxn() - 1;
                    }

                    // We're appending onto the last (active) partition.
                    final boolean append = last && (srcDataMax == 0 || (isCommitDedupMode() && o3Timestamp > maxTimestamp) || (!isCommitDedupMode() && o3Timestamp >= maxTimestamp))
                            // If it's replace commit, the append is only possible if the last partition data is
                            // before the replace range.
                            && (!isCommitReplaceMode() || o3TimestampMin > txWriter.getMaxTimestamp());

                    // Number of rows to insert from the O3 segment into this partition.
                    final long srcOooBatchRowSize = srcOooHi - srcOooLo + 1;

                    // Final partition size after current insertions.
                    long newPartitionSize = srcDataMax + srcOooBatchRowSize;

                    // check partition read-only state
                    final boolean partitionIsReadOnly = partitionIndexRaw > -1 && txWriter.isPartitionReadOnlyByRawIndex(partitionIndexRaw);
                    final boolean isParquet = partitionIndexRaw > -1 && txWriter.isPartitionParquetByRawIndex(partitionIndexRaw);

                    pCount++;

                    LOG.info().$("o3 partition task [table=").$(tableToken)
                            .$(", partitionTs=").$ts(timestampDriver, partitionTimestamp)
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
                            .$(", o3Ts=").$ts(timestampDriver, o3Timestamp)
                            .$(", newSize=").$(newPartitionSize)
                            .$(", maxTs=").$ts(timestampDriver, maxTimestamp)
                            .$(", pCount=").$(pCount)
                            .$(", flattenTs=").$(flattenTimestamp)
                            .$(", memUsed=").$size(Unsafe.getMemUsed())
                            .$(", rssMemUsed=").$size(Unsafe.getRssMemUsed())
                            .I$();

                    if (partitionIsReadOnly) {
                        // move over read-only partitions
                        LOG.critical()
                                .$("o3 ignoring write on read-only partition [table=").$(tableToken)
                                .$(", timestamp=").$ts(timestampDriver, partitionTimestamp)
                                .$(", numRows=").$(srcOooBatchRowSize)
                                .$();
                        continue;
                    }
                    final O3Basket o3Basket = o3BasketPool.next();
                    o3Basket.checkCapacity(configuration, columnCount, indexCount);
                    AtomicInteger columnCounter = o3ColumnCounters.next();

                    if (isCommitReplaceMode() && srcOooLo > srcOooHi && (srcDataMax == 0 || append)) {
                        // Nothing to insert and replace range does not intersect with existing data
                        partitionTimestamp = txWriter.getNextExistingPartitionTimestamp(partitionTimestamp);
                        pressureControl.updateInflightPartitions(--inflightPartitions);
                        continue;
                    }

                    // To collect column top values and partition updates
                    // from o3 partition tasks, add them to pre-allocated continuous block of memory
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

                    if (
                            isCommitReplaceMode()
                                    && partitionTimestamp >= lastPartitionTimestamp
                                    && o3TimestampMax >= txWriter.getMaxTimestamp()
                                    && srcOooLo < srcOooMax
                    ) {
                        replaceMaxTimestamp = getTimestampIndexValue(sortedTimestampsAddr, srcOooMax - 1);
                    }

                    if (append) {
                        // we are appending the last partition, make sure it has been mapped!
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

                        columnCounter.set(compressColumnCount(metadata));
                        Path pathToPartition = Path.getThreadLocal(path);
                        setPathForNativePartition(
                                pathToPartition,
                                timestampType,
                                partitionBy,
                                txWriter.getPartitionTimestampByTimestamp(o3TimestampMin),
                                srcNameTxn
                        );
                        final int plen = pathToPartition.size();
                        int columnsPublished = 0;
                        long minTimestamp = isCommitReplaceMode() ? txWriter.getMinTimestamp() : o3TimestampMin;
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
                                srcOooFixAddr = notTheTimestamp ? oooMem1.addressOf(0) : sortedTimestampsAddr;
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
                                        minTimestamp,
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

                        long o3TimestampLo, o3TimestampHi;
                        if (isCommitReplaceMode()) {
                            if (isParquet) {
                                // Parquet partitions do not support replace commits feature yet
                                o3PartitionUpdRemaining.decrementAndGet();
                                latchCount--;
                                pressureControl.updateInflightPartitions(--inflightPartitions);
                                throw CairoException.critical(0)
                                        .put("commit replace mode is not supported for Parquet partitions [table=").put(getTableToken().getTableName())
                                        .put(", partition=").ts(timestampDriver, partitionTimestamp).put(']');
                            }
                            o3TimestampLo = (partitionTimestamp == minO3PartitionTimestamp) ? o3TimestampMin : partitionTimestamp;
                            o3TimestampHi = (partitionTimestamp == maxO3PartitionTimestamp) ? o3TimestampMax :
                                    txWriter.getCurrentPartitionMaxTimestamp(partitionTimestamp);
                        } else {
                            o3TimestampLo = getTimestampIndexValue(sortedTimestampsAddr, srcOooLo);
                            o3TimestampHi = getTimestampIndexValue(sortedTimestampsAddr, srcOooHi);
                        }

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
                                isParquet,
                                o3TimestampLo,
                                o3TimestampHi
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
                partitionTimestamp = txWriter.getNextExistingPartitionTimestamp(partitionTimestamp);
            } // end while(srcOoo < srcOooMax)

            // at this point we should know the last partition row count
            partitionTimestampHi = Math.max(partitionTimestampHi, txWriter.getCurrentPartitionMaxTimestamp(o3TimestampMax));

            if (!isCommitReplaceMode()) {
                txWriter.updateMaxTimestamp(Math.max(txWriter.getMaxTimestamp(), o3TimestampMax));
            } else if (replaceMaxTimestamp != Long.MIN_VALUE) {
                txWriter.updateMaxTimestamp(replaceMaxTimestamp);
            }
        } catch (Throwable th) {
            LOG.error().$("failed to commit data block [table=").$(tableToken).$(", error=").$(th).I$();
            throw th;
        } finally {
            // we are stealing work here it is possible we get exception from this method
            LOG.debug()
                    .$("o3 expecting updates [table=").$(tableToken)
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
            LOG.info().$("shifting lag rows up [table=").$(tableToken).$(", lagCount=").$(o3LagRowCount).I$();
            dispatchColumnTasks(
                    o3LagRowCount,
                    IGNORE,
                    srcOooMax,
                    0L,
                    0,
                    cthO3ShiftColumnInLagToTopRef
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
                final long txn = partitionRemoveCandidates.get(i + 1);
                // txn >= lastCommittedTxn means there are some versions found in the table directory
                // that are not attached to the table most likely as a result of a rollback
                if (!anyReadersBeforeCommittedTxn || txn >= lastCommittedTxn) {
                    setPathForNativePartition(
                            other,
                            timestampType,
                            partitionBy,
                            timestamp,
                            txn
                    );
                    other.$();
                    engine.getPartitionOverwriteControl().notifyPartitionMutates(tableToken, timestampType, timestamp, txn, 0);
                    if (!ff.unlinkOrRemove(other, LOG)) {
                        LOG.info()
                                .$("could not purge partition version, async purge will be scheduled [path=").$substr(pathRootSize, other)
                                .$(", errno=").$(ff.errno())
                                .I$();
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
            if (schedulePurgeO3Partitions(messageBus, tableToken, timestampType, partitionBy)) {
                LOG.debug().$("scheduled to purge partitions [table=").$(tableToken).I$();
            } else {
                LOG.error().$("could not queue for purge, queue is full [table=").$(tableToken).I$();
            }
        }
    }

    // returns true if the tx was committed into the table and can be made visible to readers
    // returns false if the tx was only copied to LAG and not committed - in this case the tx is not visible to readers
    private boolean processWalCommit(
            @Transient Path walPath,
            boolean ordered,
            long rowLo,
            long rowHi,
            final long o3TimestampMin,
            final long o3TimestampMax,
            @Nullable SymbolMapDiffCursor mapDiffCursor,
            long commitToTimestamp,
            long walIdSegmentId,
            boolean isLastSegmentUsage,
            TableWriterPressureControl pressureControl
    ) {
        int initialSize = segmentFileCache.getWalMappedColumns().size();
        int timestampIndex = metadata.getTimestampIndex();
        int walRootPathLen = walPath.size();
        long maxTimestamp = txWriter.getMaxTimestamp();
        if (isLastPartitionClosed()) {
            if (isEmptyTable()) {
                // The table is empty, last partition does not exist
                // WAL processing needs last partition to store LAG data
                // Create artificial partition at the point of o3TimestampMin.
                openPartition(o3TimestampMin, 0);
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
            segmentFileCache.mmapSegments(metadata, walPath, walIdSegmentId, rowLo, rowHi);
            o3Columns = segmentFileCache.getWalMappedColumns();
            final long newMinLagTimestamp = Math.min(o3TimestampMin, txWriter.getLagMinTimestamp());
            long initialPartitionTimestampHi = partitionTimestampHi;

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
                        || (configuration.getWalMaxLagTxnCount() > 0 && txWriter.getLagTxnCount() >= configuration.getWalMaxLagTxnCount())
                        // when the time between commits is too long we need to commit regardless of the row count or volume filled
                        // this is to bring the latency of data visibility inline with user expectations
                        || (configuration.getMicrosecondClock().getTicks() - lastWalCommitTimestampMicros > configuration.getCommitLatency());

                boolean canFastCommit = indexers.size() == 0 && applyFromWalLagToLastPartitionPossible(commitToTimestamp, txWriter.getLagRowCount(), txWriter.isLagOrdered(), txWriter.getMaxTimestamp(), txWriter.getLagMinTimestamp(), txWriter.getLagMaxTimestamp());
                boolean lagOrderedNew = !isCommitDedupMode() && txWriter.isLagOrdered() && ordered && walLagMaxTimestampBefore <= o3TimestampMin;
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
                boolean needsDedup = isCommitDedupMode();

                long timestampAddr = 0;
                MemoryCR walTimestampColumn = segmentFileCache.getWalMappedColumns().getQuick(getPrimaryColumnIndex(timestampIndex));
                o3Columns = remapWalSymbols(mapDiffCursor, rowLo, rowHi, walPath);

                if (needsOrdering || needsDedup) {
                    if (needsOrdering) {
                        LOG.debug().$("sorting WAL [table=").$(tableToken)
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
                            long rowCount = Vect.radixSortABLongIndexAsc(
                                    Math.abs(tsLagBufferAddr),
                                    walLagRowCount,
                                    mappedTimestampIndexAddr,
                                    commitRowCount,
                                    timestampAddr,
                                    o3TimestampMemCpy.addressOf(0),
                                    txWriter.getLagMinTimestamp(),
                                    txWriter.getLagMaxTimestamp()
                            );
                            assert rowCount == totalUncommitted : "radix sort error, result: " + rowCount + " expected " + totalUncommitted;
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
                    // where access getAddress() calls are concurrent. Map them eagerly now.
                    segmentFileCache.mmapWalColsEager();

                    timestampAddr = walTimestampColumn.addressOf(0);
                    copiedToMemory = false;
                }

                // We could commit some portion of the lag into the partitions and keep some data in the lag
                // like 70/30 split, but it would break snapshot assumptions and this optimization is removed
                // in the next release after v7.3.9.
                // Commit everything.
                walLagRowCount = 0;
                processWalCommitFinishApply(walLagRowCount, timestampAddr, o3Lo, o3Hi, pressureControl, copiedToMemory, initialPartitionTimestampHi);
            } finally {
                finishO3Append(walLagRowCount);
                o3Columns = o3MemColumns1;
            }

            return true;
        } catch (Throwable th) {
            success = false;
            throw th;
        } finally {
            if (memColumnShifted) {
                clearMemColumnShifts();
            }
            walPath.trimTo(walRootPathLen);
            segmentFileCache.closeWalFiles(isLastSegmentUsage || !success, walIdSegmentId, initialSize);
        }
    }

    private boolean processWalCommit(Path walPath, long seqTxn, TableWriterPressureControl pressureControl, long commitToTimestamp) {
        int walId = walTxnDetails.getWalId(seqTxn);
        long txnMinTs = walTxnDetails.getMinTimestamp(seqTxn);
        long txnMaxTs = walTxnDetails.getMaxTimestamp(seqTxn);
        long rowLo = walTxnDetails.getSegmentRowLo(seqTxn);
        long rowHi = walTxnDetails.getSegmentRowHi(seqTxn);
        boolean inOrder = walTxnDetails.getTxnInOrder(seqTxn);
        byte dedupMode = walTxnDetails.getDedupMode(seqTxn);
        long replaceRangeTsLo = walTxnDetails.getReplaceRangeTsLow(seqTxn);
        long replaceRangeTsHi = walTxnDetails.getReplaceRangeTsHi(seqTxn);

        LogRecord logLine = LOG.info();
        logLine.$("processing WAL [path=").$substr(pathRootSize, walPath)
                .$(", roLo=").$(rowLo)
                .$(", roHi=").$(rowHi)
                .$(", seqTxn=").$(seqTxn)
                .$(", tsMin=").$ts(timestampDriver, txnMinTs).$(", tsMax=").$ts(timestampDriver, txnMaxTs)
                .$(", commitToTs=").$ts(timestampDriver, commitToTimestamp);

        if (replaceRangeTsLo < replaceRangeTsHi) {
            logLine.$(", replaceRangeTsLo=").$ts(timestampDriver, replaceRangeTsLo)
                    .$(", replaceRangeTsHi=").$ts(timestampDriver, replaceRangeTsHi);
        }
        logLine.I$();

        final int segmentId = walTxnDetails.getSegmentId(seqTxn);
        boolean isLastSegmentUsage = walTxnDetails.isLastSegmentUsage(seqTxn);
        SymbolMapDiffCursor mapDiffCursor = walTxnDetails.getWalSymbolDiffCursor(seqTxn);

        long walIdSegmentId = Numbers.encodeLowHighInts(segmentId, walId);
        walRowsProcessed = rowHi - rowLo;

        if (dedupMode == WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE) {
            processWalCommitDedupReplace(
                    walPath,
                    inOrder,
                    rowLo,
                    rowHi,
                    txnMinTs,
                    txnMaxTs,
                    mapDiffCursor,
                    walIdSegmentId,
                    isLastSegmentUsage,
                    pressureControl,
                    replaceRangeTsLo,
                    replaceRangeTsHi
            );

            // Min timestamp is either outside of replace range or equals to the min timestamp of the transaction
            assert txWriter.getMinTimestamp() < replaceRangeTsLo || txWriter.getMinTimestamp() >= replaceRangeTsHi || txWriter.getMinTimestamp() == txnMinTs;
            // Max timestamp is either outside of replace range or equals to the max timestamp of the transaction
            assert txWriter.getMaxTimestamp() < replaceRangeTsLo || txWriter.getMaxTimestamp() >= replaceRangeTsHi || txWriter.getMaxTimestamp() == txnMaxTs;

            return true;
        } else {
            return processWalCommit(
                    walPath,
                    inOrder,
                    rowLo,
                    rowHi,
                    txnMinTs,
                    txnMaxTs,
                    mapDiffCursor,
                    commitToTimestamp,
                    walIdSegmentId,
                    isLastSegmentUsage,
                    pressureControl
            );
        }
    }

    private int processWalCommitBlock(
            long startSeqTxn,
            int blockTransactionCount,
            TableWriterPressureControl pressureControl
    ) {
        segmentCopyInfo.clear();
        walTxnDetails.prepareCopySegments(startSeqTxn, blockTransactionCount, segmentCopyInfo, denseSymbolMapWriters.size() > 0);
        if (isLastPartitionClosed()) {
            if (isEmptyTable()) {
                populateDenseIndexerList();
            }
        }

        LOG.info().$("processing WAL transaction block [table=").$(tableToken)
                .$(", seqTxn=").$(startSeqTxn).$("..").$(startSeqTxn + blockTransactionCount - 1)
                .$(", rows=").$(segmentCopyInfo.getTotalRows())
                .$(", segments=").$(segmentCopyInfo.getSegmentCount())
                .$(", minTimestamp=").$ts(timestampDriver, segmentCopyInfo.getMinTimestamp())
                .$(", maxTimestamp=").$ts(timestampDriver, segmentCopyInfo.getMaxTimestamp())
                .I$();

        walRowsProcessed = segmentCopyInfo.getTotalRows();
        if (segmentCopyInfo.hasSegmentGaps()) {
            LOG.info().$("some segments have gaps in committed rows [table=").$(tableToken).I$();
            throw CairoException.txnApplyBlockError(tableToken);
        }

        // Don't move the line to mmap Wal column inside the following try block,
        // This call, if failed will close the WAL files correctly on its own
        // putting it inside the try block will cause the WAL files to be closed twice in the finally block
        // in case of the exception.
        segmentFileCache.mmapWalColumns(segmentCopyInfo, metadata, path);
        try {
            final long timestampAddr;
            final boolean copiedToMemory;
            final long o3Lo;
            final long o3LoHi;

            if (!isCommitDedupMode() && segmentCopyInfo.getAllTxnDataInOrder() && segmentCopyInfo.getSegmentCount() == 1) {
                LOG.info().$("all data in order, single segment, processing optimised [table=").$(tableToken).I$();
                // all data comes from a single segment and is already sorted
                if (denseSymbolMapWriters.size() > 0) {
                    segmentFileCache.mmapWalColsEager();
                    o3Columns = processWalCommitBlock_remapSymbols();
                } else {
                    // No symbols, nothing to remap
                    segmentFileCache.mmapWalColsEager();
                    o3Columns = segmentFileCache.getWalMappedColumns();
                }

                // There is only one segment
                o3Lo = segmentCopyInfo.getRowLo(0);
                o3LoHi = o3Lo + segmentCopyInfo.getTotalRows();
                MemoryCR tsColumn = o3Columns.get(getPrimaryColumnIndex(metadata.getTimestampIndex()));
                timestampAddr = tsColumn.addressOf(0);
                txWriter.setLagMinTimestamp(segmentCopyInfo.getMinTimestamp());
                txWriter.setLagMaxTimestamp(segmentCopyInfo.getMaxTimestamp());
                copiedToMemory = false;
            } else {
                o3Lo = 0;
                o3LoHi = processWalCommitBlock_sortWalSegmentTimestamps();
                timestampAddr = o3TimestampMem.getAddress();
                copiedToMemory = true;
            }

            try {
                lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();
                processWalCommitFinishApply(
                        0,
                        timestampAddr,
                        o3Lo,
                        o3LoHi,
                        pressureControl,
                        copiedToMemory,
                        partitionTimestampHi
                );
            } finally {
                finishO3Append(0);
                o3Columns = o3MemColumns1;
            }
            return blockTransactionCount;
        } finally {
            if (memColumnShifted) {
                clearMemColumnShifts();
            }
            segmentFileCache.closeWalFiles(segmentCopyInfo, metadata.getColumnCount());
            if (tempDirectMemList != null) {
                tempDirectMemList.resetCapacity();
            }
        }
    }

    private ObjList<MemoryCR> processWalCommitBlock_remapSymbols() {
        long columnAddressCount = 2L * metadata.getColumnCount();
        DirectLongList columnSegmentAddressBuffer = getTempDirectLongList(columnAddressCount);
        columnSegmentAddressBuffer.setPos(columnAddressCount);

        // Remap the symbols if there are any
        processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks(
                0,
                segmentCopyInfo.getTotalRows(),
                columnSegmentAddressBuffer.getAddress(),
                1,
                segmentCopyInfo.getRowLo(0),
                true,
                false,
                cthMapSymbols
        );

        o3ColumnOverrides.clear();
        o3ColumnOverrides.addAll(segmentFileCache.getWalMappedColumns());

        long columnSegmentAddressesBase = columnSegmentAddressBuffer.getAddress();
        // Take symbol columns from o3MemColumns2 if remapping happened
        int tsColIndex = metadata.getTimestampIndex();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            int columnType = metadata.getColumnType(i);
            if (i != tsColIndex && columnType > 0) {
                if (ColumnType.isSymbol(columnType)) {
                    // -1 is the indicator that column is remapped.
                    // Otherwise, the symbol remapping did not happen because there were no new symbols
                    if (Unsafe.getUnsafe().getLong(columnSegmentAddressesBase) == -1L) {
                        int primaryColumnIndex = getPrimaryColumnIndex(i);
                        MemoryCARW remappedColumn = o3MemColumns2.getQuick(primaryColumnIndex);
                        assert remappedColumn.size() >= (segmentCopyInfo.getTotalRows() << ColumnType.pow2SizeOf(columnType));
                        o3ColumnOverrides.set(primaryColumnIndex, remappedColumn);

                        LOG.info().$("using remapped column buffer [table=").$(tableToken)
                                .$(", name=").$safe(metadata.getColumnName(i)).$(", index=").$(i).I$();
                    }
                }
                columnSegmentAddressesBase += Long.BYTES;
                if (ColumnType.isVarSize(columnType)) {
                    columnSegmentAddressesBase += Long.BYTES;
                }
            }
        }
        columnSegmentAddressBuffer.resetCapacity();
        return o3ColumnOverrides;
    }

    // The name of the method starts from processWalCommitBlock to keep the methods close together after code formatting
    private long processWalCommitBlock_sortWalSegmentTimestamps() {
        int timestampIndex = metadata.getTimestampIndex();

        // For each column, we will need a dense list of column addresses
        // across all the segments. It will be used in column shuffling/remapping procs
        // Allocate enough space to store all the addresses, e.g., for every segment 2x long for every column.
        int walColumnCountPerSegment = metadata.getColumnCount() * 2;
        long totalSegmentAddresses = segmentCopyInfo.getSegmentCount();
        long totalColumnAddressSize = totalSegmentAddresses * walColumnCountPerSegment;
        DirectLongList tsAddresses = getTempDirectLongList(totalColumnAddressSize);
        tsAddresses.setPos(totalColumnAddressSize);

        try {
            segmentFileCache.createAddressBuffersPrimary(timestampIndex, metadata.getColumnCount(), segmentCopyInfo.getSegmentCount(), tsAddresses.getAddress());
            long totalRows = segmentCopyInfo.getTotalRows();
            // This sort apart from creating normal merge index with 2 longs
            // creates reverse long index at the end, e.g. it needs one more long per row
            // It does not need all 64 bits, most of the cases it can do with 32bit but here we over allocate
            // and let native code to use what's needed
            // One more 64bit is used to add additional flags the native code passes
            // between sort and shuffle calls about the format of the index
            // e.g. how many bits is used for segment index values
            o3TimestampMem.jumpTo(totalRows * Long.BYTES * 3 + Long.BYTES);
            o3TimestampMemCpy.jumpTo(totalRows * Long.BYTES * 3 + Long.BYTES);

            long timestampAddr = o3TimestampMem.getAddress();

            boolean needsDedup = isDeduplicationEnabled();
            long minTs = segmentCopyInfo.getMinTimestamp();
            long maxTs = segmentCopyInfo.getMaxTimestamp();

            LOG.debug().$("sorting [table=").$(tableToken)
                    .$(", rows=").$(segmentCopyInfo.getTotalRows())
                    .$(", segments=").$(segmentCopyInfo.getSegmentCount())
                    .$(", minTs=").$ts(timestampDriver, minTs)
                    .$(", maxTs=").$ts(timestampDriver, maxTs)
                    .I$();

            long indexFormat = Vect.radixSortManySegmentsIndexAsc(
                    timestampAddr,
                    o3TimestampMemCpy.addressOf(0),
                    tsAddresses.getAddress(),
                    (int) tsAddresses.size(),
                    segmentCopyInfo.getTxnInfoAddress(),
                    segmentCopyInfo.getTxnCount(),
                    segmentCopyInfo.getMaxTxRowCount(),
                    0,
                    0,
                    minTs,
                    maxTs,
                    totalRows,
                    needsDedup ? Vect.DEDUP_INDEX_FORMAT : Vect.SHUFFLE_INDEX_FORMAT
            );

            // the result of the sort is sort index. The format of the index is different
            // if the dedup is needed. See comments on Vect.DEDUP_INDEX_FORMAT and Vect.SHUFFLE_INDEX_FORMAT
            // to understand the difference

            // Clean timestamp addresses, otherwise the shuffle code will lazily re-use them
            tsAddresses.set(0, 0L);

            if (!Vect.isIndexSuccess(indexFormat)) {
                LOG.info().$("transaction sort error, will switch to 1 commit [table=").$(tableToken)
                        .$(", totalRows=").$(totalRows)
                        .$(", indexFormat=").$(indexFormat)
                        .I$();

                throw CairoException.txnApplyBlockError(tableToken);
            }

            if (isDeduplicationEnabled()) {
                if (denseSymbolMapWriters.size() > 0 && dedupColumnCommitAddresses.getColumnCount() > 0) {
                    // Remap the symbols if there are any SYMBOL dedup keys
                    processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks(
                            timestampAddr,
                            totalRows,
                            tsAddresses.getAddress(),
                            totalSegmentAddresses,
                            0,
                            true,
                            true,
                            cthMapSymbols
                    );
                }

                indexFormat = processWalCommitBlock_sortWalSegmentTimestamps_deduplicateSortedIndexFromManyAddresses(
                        indexFormat,
                        timestampAddr,
                        o3TimestampMemCpy.addressOf(0),
                        tsAddresses.getAddress()
                );

                // The last call to dedup the index converts it to the shuffle format.
                // See comments on Vect.DEDUP_INDEX_FORMAT and Vect.SHUFFLE_INDEX_FORMAT
                // to understand the difference

                if (!Vect.isIndexSuccess(indexFormat)) {
                    LOG.critical().$("WAL dedup sorted index failed will switch to 1 commit [table=").$(tableToken)
                            .$(", totalRows=").$(totalRows)
                            .$(", indexFormat=").$(indexFormat)
                            .I$();

                    throw CairoException.txnApplyBlockError(tableToken);
                }

                long dedupedRowCount = Vect.readIndexResultRowCount(indexFormat);
                LOG.info().$("WAL dedup sorted commit index [table=").$(tableToken)
                        .$(", totalRows=").$(totalRows)
                        .$(", dups=").$(totalRows - dedupedRowCount)
                        .I$();

                // Swap tsMemory and tsMemoryCopy, result of index dedup is in o3TimestampMemCpy
                timestampAddr = swapTimestampInMemCols(timestampIndex);
                totalRows = dedupedRowCount;
            }

            LOG.debug().$("shuffling [table=").$(tableToken).$(", columCount=")
                    .$(metadata.getColumnCount() - 1)
                    .$(", rows=").$(totalRows)
                    .$(", indexFormat=").$(indexFormat).I$();

            processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks(
                    timestampAddr,
                    totalRows,
                    tsAddresses.getAddress(),
                    totalSegmentAddresses,
                    indexFormat,
                    false,
                    false,
                    cthMergeWalColumnManySegments
            );

            assert o3MemColumns1.get(getPrimaryColumnIndex(timestampIndex)) == o3TimestampMem;
            assert o3MemColumns2.get(getPrimaryColumnIndex(timestampIndex)) == o3TimestampMemCpy;

            o3Columns = o3MemColumns1;
            activeColumns = o3MemColumns1;

            txWriter.setLagMinTimestamp(minTs);
            txWriter.setLagMaxTimestamp(maxTs);

            return totalRows;
        } finally {
            tsAddresses.resetCapacity();
        }
    }

    private long processWalCommitBlock_sortWalSegmentTimestamps_deduplicateSortedIndexFromManyAddresses(
            long indexFormat,
            long srcIndexSrcAddr,
            long outIndexAddr,
            long columnAddressBufferPrimary
    ) {
        int dedupKeyIndex = 0;
        long dedupCommitAddr = 0;
        try {
            if (dedupColumnCommitAddresses.getColumnCount() > 0) {
                dedupCommitAddr = dedupColumnCommitAddresses.allocateBlock();
                int columnCount = metadata.getColumnCount();
                int bytesPerColumn = segmentCopyInfo.getSegmentCount() * Long.BYTES;

                for (int i = 0; i < columnCount; i++) {
                    int columnType = metadata.getColumnType(i);
                    if (i != metadata.getTimestampIndex() && columnType > 0) {
                        if (metadata.isDedupKey(i)) {
                            long dataAddresses;
                            int valueSizeBytes = ColumnType.isVarSize(columnType) ? -1 : ColumnType.sizeOf(columnType);

                            if (!ColumnType.isSymbol(columnType) || Unsafe.getUnsafe().getLong(columnAddressBufferPrimary) == 0) {
                                segmentFileCache.createAddressBuffersPrimary(i, columnCount, segmentCopyInfo.getSegmentCount(), columnAddressBufferPrimary);
                                dataAddresses = columnAddressBufferPrimary;
                            } else {
                                // Symbols are already re-mapped, and the buffer is in o3ColumnMem2
                                dataAddresses = o3MemColumns2.get(getPrimaryColumnIndex(i)).addressOf(0);
                                // Indicate that it's a special case of the column type; only one address is supplied
                                valueSizeBytes = -1;
                            }

                            long addr = DedupColumnCommitAddresses.setColValues(
                                    dedupCommitAddr,
                                    dedupKeyIndex++,
                                    columnType,
                                    valueSizeBytes,
                                    0L
                            );

                            if (!ColumnType.isVarSize(columnType)) {
                                DedupColumnCommitAddresses.setColAddressValues(addr, dataAddresses);
                            } else {
                                long columnAddressBufferSecondary = columnAddressBufferPrimary + bytesPerColumn;
                                ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                                segmentFileCache.createAddressBuffersPrimary(i, columnCount, segmentCopyInfo.getSegmentCount(), columnAddressBufferPrimary);
                                segmentFileCache.createAddressBuffersSecondary(i, columnCount, segmentCopyInfo, columnAddressBufferSecondary, driver);

                                DedupColumnCommitAddresses.setColAddressValues(addr, columnAddressBufferSecondary, columnAddressBufferPrimary, 0L);
                                DedupColumnCommitAddresses.setO3DataAddressValues(addr, DedupColumnCommitAddresses.NULL, DedupColumnCommitAddresses.NULL, 0);
                            }
                        }

                        // Reserve segment address buffers even for non-dedup columns
                        // They will be re-used on shuffling
                        columnAddressBufferPrimary += bytesPerColumn;
                        if (ColumnType.isVarSize(columnType)) {
                            // For secondary
                            columnAddressBufferPrimary += bytesPerColumn;
                        }
                    }
                }
                assert dedupKeyIndex <= dedupColumnCommitAddresses.getColumnCount();
            }
            return Vect.dedupSortedTimestampIndexManyAddresses(
                    indexFormat,
                    srcIndexSrcAddr,
                    outIndexAddr,
                    dedupKeyIndex,
                    DedupColumnCommitAddresses.getAddress(dedupCommitAddr)
            );
        } finally {
            dedupColumnCommitAddresses.clear();
        }
    }

    private void processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks(
            long timestampAddr,
            long totalRows,
            long columnAddressesBuffer,
            long totalSegmentAddresses,
            long indexFormatOrSymbolRowsOffset,
            boolean symbolColumnsOnly,
            boolean dedupColumnOnly,
            ColumnTaskHandler taskHandler
    ) {
        // Dispatch processWalCommitBlock column tasks.
        // This method is used in 3 places:
        // 1. Remap symbols to table symbol ids before deduplication
        // 2. Remap symbols to table symbol ids when all transactions are from the same segment and data is in order
        // 2. Shuffle columns data according to the index
        final long timestampColumnIndex = metadata.getTimestampIndex();
        final Sequence pubSeq = this.messageBus.getColumnTaskPubSeq();
        final RingQueue<ColumnTask> queue = this.messageBus.getColumnTaskQueue();
        o3DoneLatch.reset();
        o3ErrorCount.set(0);
        lastErrno = 0;
        int queuedCount = 0;

        long totalSegmentAddressesBytes = totalSegmentAddresses * Long.BYTES;
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = metadata.getColumnType(columnIndex);
            if (columnIndex != timestampColumnIndex && columnType > 0) {

                // Allocate the column buffers the same way
                // regardless if it is a symbol only pass or all column pass,
                // e.g., allocate addresses for non-deleted  columns.
                // After symbol only remap pass, the buffer addresses
                // will contain a hint that the symbol remapping is already done and the shuffle proc rely on that flag.

                long mappedAddrBuffPrimary = columnAddressesBuffer;
                columnAddressesBuffer += totalSegmentAddressesBytes;
                if (ColumnType.isVarSize(columnType)) {
                    columnAddressesBuffer += totalSegmentAddressesBytes;
                }

                if ((!symbolColumnsOnly || ColumnType.isSymbol(columnType)) && (!dedupColumnOnly || metadata.isDedupKey(columnIndex))) {
                    long cursor = pubSeq.next();

                    if (cursor > -1) {
                        try {
                            final ColumnTask task = queue.get(cursor);
                            task.of(
                                    o3DoneLatch,
                                    columnIndex,
                                    columnType,
                                    timestampColumnIndex,
                                    timestampAddr,
                                    mappedAddrBuffPrimary,
                                    totalRows,
                                    indexFormatOrSymbolRowsOffset,
                                    totalSegmentAddressesBytes,
                                    taskHandler
                            );
                        } finally {
                            queuedCount++;
                            pubSeq.done(cursor);
                        }
                    } else {
                        taskHandler.run(
                                columnIndex,
                                columnType,
                                timestampColumnIndex,
                                timestampAddr,
                                mappedAddrBuffPrimary,
                                totalRows,
                                indexFormatOrSymbolRowsOffset,
                                totalSegmentAddressesBytes
                        );
                    }
                }
            }
        }
        consumeColumnTasks(queue, queuedCount);
    }

    // remap symbols column into o3MemColumns2
    private void processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks_mapSymbols(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long mergeIndex,
            long mappedAddrBuffPrimary,
            long totalRows,
            long rowsOffset,
            long totalSegmentAddressesBytes
    ) {
        try {
            assert ColumnType.isSymbol(columnType);
            final int shl = ColumnType.pow2SizeOf(columnType);

            var mapWriter = symbolMapWriters.get(columnIndex);

            // Use o3MemColumns1 to build symbol maps.
            // and o3MemColumns2 to store re-mapped symbols
            var symbolMapMem = o3MemColumns1.get(getPrimaryColumnIndex(columnIndex));
            symbolMapMem.jumpTo(0);

            boolean needsRemapping = walTxnDetails.buildTxnSymbolMap(segmentCopyInfo, columnIndex, mapWriter, symbolMapMem);

            // When there are no new symbols for all the segment transactions, remapping is not needed and not performed
            if (needsRemapping) {
                long txnCount = this.segmentCopyInfo.getTxnCount();
                segmentFileCache.createAddressBuffersPrimary(columnIndex, metadata.getColumnCount(), segmentCopyInfo.getSegmentCount(), mappedAddrBuffPrimary);
                var destinationColumn = o3MemColumns2.get(getPrimaryColumnIndex(columnIndex));

                destinationColumn.jumpTo((rowsOffset + totalRows) << shl);

                LOG.debug().$("remapping WAL symbols [table=").$(tableToken)
                        .$(", column=").$safe(metadata.getColumnName(columnIndex))
                        .$(", rows=").$(totalRows)
                        .$(", txnCount=").$(txnCount)
                        .$(", offset=").$(rowsOffset)
                        .I$();

                long rowCount = Vect.remapSymbolColumnFromManyAddresses(
                        mappedAddrBuffPrimary,
                        destinationColumn.getAddress(),
                        segmentCopyInfo.getTxnInfoAddress(),
                        txnCount,
                        symbolMapMem.getAddress()
                );
                destinationColumn.shiftAddressRight(rowsOffset << shl);
                memColumnShifted |= (rowsOffset << shl) != 0;

                if (rowCount != totalRows) {
                    throwApplyBlockColumnShuffleFailed(columnIndex, columnType, totalRows, rowCount);
                }

                // Save the hint that this symbol is already re-mapped, and the results are in o3MemColumns2
                Unsafe.getUnsafe().putLong(mappedAddrBuffPrimary, -1L);
            } else {
                // Save the hint that symbol column is not re-mapped
                Unsafe.getUnsafe().putLong(mappedAddrBuffPrimary, 0);
                LOG.debug().$("no new symbols, no remapping needed [table=").$(tableToken)
                        .$(", column=").$safe(metadata.getColumnName(columnIndex))
                        .$(", rows=").$(totalRows)
                        .I$();
            }
        } catch (Throwable th) {
            handleColumnTaskException(
                    "could not copy remap WAL symbols",
                    columnIndex,
                    columnType,
                    totalRows,
                    mergeIndex,
                    rowsOffset,
                    totalSegmentAddressesBytes,
                    th
            );
        }
    }

    private void processWalCommitBlock_sortWalSegmentTimestamps_dispatchColumnSortTasks_mergeShuffleWalColumnManySegments(
            int columnIndex,
            int columnType,
            long timestampColumnIndex,
            long mergeIndexAddress,
            long mappedAddrBuffPrimary,
            long totalRows,
            long mergeIndexFormat,
            long totalSegmentAddressesBytes
    ) {
        try {
            boolean varSize = ColumnType.isVarSize(columnType);
            final int shl = ColumnType.pow2SizeOf(columnType);

            // If this column used for deduplication, the pointers are already created
            long firstPointer = Unsafe.getUnsafe().getLong(mappedAddrBuffPrimary);
            boolean pointersNotCreated = firstPointer == 0;
            if (pointersNotCreated) {
                segmentFileCache.createAddressBuffersPrimary(columnIndex, metadata.getColumnCount(), segmentCopyInfo.getSegmentCount(), mappedAddrBuffPrimary);
            } else {
                // This should only be the case when table already went thought deduplication
                assert isDeduplicationEnabled() && metadata.isDedupKey(columnIndex);
            }

            if (!varSize) {
                if (!ColumnType.isSymbol(columnType)) {
                    // When dedup is enabled, all symbols are already remapped at this point
                    var destinationColumn = o3MemColumns1.get(getPrimaryColumnIndex(columnIndex));
                    destinationColumn.jumpTo(totalRows << shl);

                    long rowCount = Vect.mergeShuffleFixedColumnFromManyAddresses(
                            (1 << shl),
                            mergeIndexFormat,
                            mappedAddrBuffPrimary,
                            destinationColumn.getAddress(),
                            mergeIndexAddress,
                            segmentCopyInfo.getSegmentsAddress(),
                            segmentCopyInfo.getSegmentCount()
                    );

                    if (rowCount != totalRows) {
                        throwApplyBlockColumnShuffleFailed(columnIndex, columnType, totalRows, rowCount);
                    }
                } else {
                    var destinationColumn = o3MemColumns1.get(getPrimaryColumnIndex(columnIndex));
                    destinationColumn.jumpTo(totalRows << shl);

                    // Symbol can be already re-mapped if it's a dedup key, they are stored in o3MemColumn2
                    // and only need to be re-shuffled
                    if (firstPointer == -1) {
                        assert isDeduplicationEnabled() && metadata.isDedupKey(columnIndex);
                        long rowCount = Vect.shuffleSymbolColumnByReverseIndex(
                                mergeIndexFormat,
                                o3MemColumns2.get(getPrimaryColumnIndex(columnIndex)).getAddress(),
                                destinationColumn.getAddress(),
                                mergeIndexAddress
                        );

                        if (rowCount != totalRows) {
                            throwApplyBlockColumnShuffleFailed(columnIndex, columnType, totalRows, rowCount);
                        }
                    } else {

                        // Also, dedup key symbol can be not re-mapped since re-mapping was not necessary,
                        // but in this case they are not stored in o3MemColumn1 and needs to be shuffled as a 32bit column
                        boolean needsRemapping = !metadata.isDedupKey(columnIndex) || !isDeduplicationEnabled();

                        // Symbols need remapping. Create mapping from transaction symbol keys to column symbol keys
                        var mapWriter = symbolMapWriters.get(columnIndex);
                        long txnCount = this.segmentCopyInfo.getTxnCount();

                        var symbolMapMem = o3MemColumns2.get(getPrimaryColumnIndex(columnIndex));
                        symbolMapMem.jumpTo(0);

                        if (needsRemapping) {
                            // If there are no new symbols, no remapping needed
                            needsRemapping = walTxnDetails.buildTxnSymbolMap(segmentCopyInfo, columnIndex, mapWriter, symbolMapMem);
                        }

                        long rowCount;
                        if (needsRemapping) {
                            rowCount = Vect.mergeShuffleSymbolColumnFromManyAddresses(
                                    mergeIndexFormat,
                                    mappedAddrBuffPrimary,
                                    destinationColumn.getAddress(),
                                    mergeIndexAddress,
                                    segmentCopyInfo.getTxnInfoAddress(),
                                    txnCount,
                                    symbolMapMem.getAddress(),
                                    symbolMapMem.getAppendOffset()
                            );
                        } else {
                            // Shuffle as int32, no new symbol values added
                            rowCount = Vect.mergeShuffleFixedColumnFromManyAddresses(
                                    (1 << shl),
                                    mergeIndexFormat,
                                    mappedAddrBuffPrimary,
                                    destinationColumn.getAddress(),
                                    mergeIndexAddress,
                                    segmentCopyInfo.getSegmentsAddress(),
                                    segmentCopyInfo.getSegmentCount()
                            );

                        }
                        if (rowCount != totalRows) {
                            throwApplyBlockColumnShuffleFailed(columnIndex, columnType, totalRows, rowCount);
                        }
                    }
                }
            } else {
                long mappedAddrBuffSecondary = mappedAddrBuffPrimary + totalSegmentAddressesBytes;
                var destinationColumnSecondary = o3MemColumns1.get(getSecondaryColumnIndex(columnIndex));
                ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                long totalVarSize = segmentFileCache.createAddressBuffersSecondary(columnIndex, metadata.getColumnCount(), segmentCopyInfo, mappedAddrBuffSecondary, driver);

                destinationColumnSecondary.jumpTo(driver.getAuxVectorSize(totalRows));
                var destinationColumnPrimary = o3MemColumns1.get(getPrimaryColumnIndex(columnIndex));
                destinationColumnPrimary.jumpTo(totalVarSize);

                long rowCount = driver.mergeShuffleColumnFromManyAddresses(
                        mergeIndexFormat,
                        mappedAddrBuffPrimary,
                        mappedAddrBuffSecondary,
                        destinationColumnPrimary.getAddress(),
                        destinationColumnSecondary.getAddress(),
                        mergeIndexAddress,
                        0,
                        totalVarSize
                );

                if (rowCount != totalRows) {
                    throwApplyBlockColumnShuffleFailed(columnIndex, columnType, totalRows, rowCount);
                }
            }
        } catch (Throwable th) {
            handleColumnTaskException(
                    "could not copy shuffle WAL segment columns",
                    columnIndex,
                    columnType,
                    totalRows,
                    mergeIndexAddress,
                    mergeIndexFormat,
                    totalSegmentAddressesBytes,
                    th
            );
        }
    }

    private void processWalCommitDedupReplace(
            @Transient Path walPath,
            boolean ordered,
            long rowLo,
            long rowHi,
            final long o3TimestampMin,
            final long o3TimestampMax,
            @Nullable SymbolMapDiffCursor mapDiffCursor,
            long walIdSegmentId,
            boolean isLastSegmentUsage,
            TableWriterPressureControl pressureControl,
            long replaceRangeTsLow,
            long replaceRangeTsHiExcl
    ) {
        assert txWriter.getLagRowCount() == 0;
        assert replaceRangeTsHiExcl <= replaceRangeTsLow || replaceRangeTsLow <= o3TimestampMin;
        assert replaceRangeTsHiExcl <= replaceRangeTsLow || replaceRangeTsHiExcl > o3TimestampMax;
        long replaceRangeTsHi = replaceRangeTsHiExcl - 1;

        int initialSize = segmentFileCache.getWalMappedColumns().size();
        int timestampIndex = metadata.getTimestampIndex();
        int walRootPathLen = walPath.size();
        boolean success = true;
        lastPartitionTimestamp = txWriter.getLastPartitionTimestamp();

        if (isLastPartitionClosed() && isEmptyTable()) {
            populateDenseIndexerList();
        }

        try {
            this.dedupMode = WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

            if (rowLo < rowHi) {
                segmentFileCache.mmapSegments(metadata, walPath, walIdSegmentId, rowLo, rowHi);
                o3Columns = segmentFileCache.getWalMappedColumns();

                try {
                    MemoryCR walTimestampColumn = segmentFileCache.getWalMappedColumns().getQuick(getPrimaryColumnIndex(timestampIndex));
                    o3Columns = remapWalSymbols(mapDiffCursor, rowLo, rowHi, walPath);

                    if (ordered) {
                        long timestampAddr = walTimestampColumn.addressOf(0);

                        txWriter.setLagMinTimestamp(replaceRangeTsLow);
                        txWriter.setLagMaxTimestamp(replaceRangeTsHi);

                        // WAL columns are lazily mapped to improve performance. It works ok, except in this case
                        // where access getAddress() calls are concurrent. Map them eagerly now.
                        segmentFileCache.mmapWalColsEager();

                        processWalCommitFinishApply(0, timestampAddr, rowLo, rowHi, pressureControl, false, partitionTimestampHi);
                    } else {
                        LOG.debug().$("sorting WAL [table=").$(tableToken)
                                .$(", ordered=false")
                                .$(", walRowLo=").$(rowLo)
                                .$(", walRowHi=").$(rowHi)
                                .I$();

                        long totalUncommitted = rowHi - rowLo;
                        final long timestampMemorySize = totalUncommitted << 4;
                        o3TimestampMem.jumpTo(timestampMemorySize);
                        o3TimestampMemCpy.jumpTo(timestampMemorySize);

                        final long mappedTimestampIndexAddr = walTimestampColumn.addressOf(rowLo << 4);
                        long timestampAddr = o3TimestampMem.getAddress();

                        long rowCount = Vect.radixSortABLongIndexAsc(
                                0,
                                0,
                                mappedTimestampIndexAddr,
                                totalUncommitted,
                                timestampAddr,
                                o3TimestampMemCpy.addressOf(0),
                                o3TimestampMin,
                                o3TimestampMax
                        );
                        assert rowCount == totalUncommitted : "radix sort error, result: " + rowCount + " expected " + totalUncommitted;
                        dispatchColumnTasks(timestampAddr, totalUncommitted, 0, rowLo, rowHi, cthMergeWalColumnWithLag);
                        swapO3ColumnsExcept(timestampIndex);
                        o3Columns = o3MemColumns1;

                        txWriter.setLagMinTimestamp(replaceRangeTsLow);
                        txWriter.setLagMaxTimestamp(replaceRangeTsHi);

                        processWalCommitFinishApply(0, timestampAddr, 0, totalUncommitted, pressureControl, true, partitionTimestampHi);
                    }
                } finally {
                    finishO3Append(0);
                    o3Columns = o3MemColumns1;
                }

            } else {
                // It is possible to get empty commit that deletes data range without adding anything, handle it here.
                try {
                    txWriter.setLagMinTimestamp(replaceRangeTsLow);
                    txWriter.setLagMaxTimestamp(replaceRangeTsHi);
                    processWalCommitFinishApply(0, 0, 0, 0, pressureControl, true, partitionTimestampHi);
                } finally {
                    finishO3Append(0);
                }
            }
        } catch (Throwable th) {
            success = false;
            throw th;
        } finally {
            this.dedupMode = WalUtils.WAL_DEDUP_MODE_DEFAULT;
            if (memColumnShifted) {
                clearMemColumnShifts();
            }
            walPath.trimTo(walRootPathLen);
            if (rowLo < rowHi) {
                segmentFileCache.closeWalFiles(isLastSegmentUsage || !success, walIdSegmentId, initialSize);
            }
        }
    }

    private void processWalCommitFinishApply(
            long walLagRowCount,
            long timestampAddr,
            long o3Lo,
            long o3Hi,
            TableWriterPressureControl pressureControl,
            boolean copiedToMemory,
            long initialPartitionTimestampHi
    ) {
        long commitMinTimestamp = txWriter.getLagMinTimestamp();
        long commitMaxTimestamp = txWriter.getLagMaxTimestamp();
        txWriter.setLagMinTimestamp(Long.MAX_VALUE);
        txWriter.setLagMaxTimestamp(Long.MIN_VALUE);

        o3RowCount = o3Hi - o3Lo + walLagRowCount;

        // Real data writing into table happens here.
        // Everything above it is to figure out how much data to write now,
        // map symbols and sort data if necessary.
        if (o3Hi > o3Lo || isCommitReplaceMode()) {
            // Now that everything from WAL lag is in memory or in WAL columns,
            // we can remove artificial 0-length partition created to store lag when table did not have any partitions
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
                    metadata.getTimestampIndex(),
                    timestampAddr,
                    o3Hi,
                    commitMinTimestamp,
                    commitMaxTimestamp,
                    copiedToMemory,
                    o3Lo,
                    pressureControl
            );

            finishO3Commit(initialPartitionTimestampHi);
        }
        txWriter.setLagOrdered(true);
        txWriter.setLagRowCount((int) walLagRowCount);
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
                        .$(", table=").$(tableToken)
                        .$(", tableId=").$(tableId)
                        .$(", correlationId=").$(correlationId);
                if (errorCode != AsyncWriterCommand.Error.OK) {
                    lg.$(",errorCode=").$(errorCode).$(",errorMsg=").$(errorMsg);
                }
                lg.I$();
            }
        } else {
            // The queue is full
            LOG.error()
                    .$("could not publish sync command complete event [type=").$(cmdType)
                    .$(", table=").$(tableToken)
                    .$(", tableId=").$(tableId)
                    .$(", correlationId=").$(correlationId)
                    .I$();
        }
    }

    private long readMinTimestamp() {
        other.of(path).trimTo(pathSize); // reset the path to table root
        final long timestamp = txWriter.getPartitionTimestampByIndex(1);
        final boolean isParquet = txWriter.isPartitionParquet(1);
        try {
            setStateForTimestamp(other, timestamp);
            return isParquet ? readMinTimestampParquet(other) : readMinTimestampNative(other, timestamp);
        } finally {
            other.trimTo(pathSize);
        }
    }

    private long readMinTimestampNative(Path partitionPath, long partitionTimestamp) {
        if (ff.exists(dFile(partitionPath, metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE))) {
            // read min timestamp value
            final long fd = openRO(ff, partitionPath.$(), LOG);
            try {
                return readLongOrFail(ff, fd, 0, tempMem16b, partitionPath.$());
            } finally {
                ff.close(fd);
            }
        } else {
            LOG.error().$("cannot read next partition min timestamp on attempt to drop the partition, next partition does not exist [path=").$(other).I$();
            return partitionTimestamp;
        }
    }

    private long readMinTimestampParquet(Path partitionPath) {
        long parquetAddr = 0;
        long parquetSize = 0;
        try {
            parquetSize = txWriter.getPartitionParquetFileSize(1);
            parquetAddr = mapRO(ff, partitionPath.concat(PARQUET_PARTITION_NAME).$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            parquetDecoder.of(parquetAddr, parquetSize, MemoryTag.NATIVE_TABLE_WRITER);
            final int timestampIndex = metadata.getTimestampIndex();
            parquetColumnIdsAndTypes.clear();
            parquetColumnIdsAndTypes.add(timestampIndex);
            parquetColumnIdsAndTypes.add(timestampType);
            parquetDecoder.readRowGroupStats(parquetStatBuffers, parquetColumnIdsAndTypes, 0);
            return parquetStatBuffers.getMinValueLong(0);
        } finally {
            if (parquetAddr != 0) {
                ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            Misc.free(parquetDecoder);
        }
    }

    private void readNativeMinMaxTimestamps(Path partitionPath, CharSequence columnName, long partitionSize) {
        final long fd = openRO(ff, dFile(partitionPath, columnName, COLUMN_NAME_TXN_NONE), LOG);
        try {
            attachMinTimestamp = ff.readNonNegativeLong(fd, 0);
            attachMaxTimestamp = ff.readNonNegativeLong(fd, (partitionSize - 1) * ColumnType.sizeOf(timestampType));
        } finally {
            ff.close(fd);
        }
    }

    // Scans timestamp file
    // returns size of partition detected, e.g., size of monotonic increase
    // of timestamp longs read from 0 offset to the end of the file
    // It also writes min and max values found in detachedMinTimestamp and detachedMaxTimestamp
    private long readNativeSizeMinMaxTimestamps(long partitionTimestamp, Path path, CharSequence columnName) {
        int pathLen = path.size();
        try {
            path.concat(TXN_FILE_NAME);
            if (ff.exists(path.$())) {
                if (attachTxReader == null) {
                    attachTxReader = new TxReader(ff);
                }
                attachTxReader.ofRO(path.$(), timestampType, partitionBy);
                attachTxReader.unsafeLoadAll();

                try {
                    path.trimTo(pathLen);
                    long partitionSize = attachTxReader.getPartitionRowCountByTimestamp(partitionTimestamp);
                    if (partitionSize <= 0) {
                        throw CairoException.nonCritical()
                                .put("partition is not preset in detached txn file [path=").put(path)
                                .put(", partitionSize=").put(partitionSize)
                                .put(']');
                    }

                    // Read min and max timestamp values from the file
                    readPartitionMinMaxTimestamps(partitionTimestamp, path.trimTo(pathLen), columnName, -1, partitionSize);
                    return partitionSize;
                } finally {
                    Misc.free(attachTxReader);
                }
            }

            // No txn file found, scan the file to get min; max timestamp
            // Scan forward while value increases
            final long fd = openRO(ff, dFile(path.trimTo(pathLen), columnName, COLUMN_NAME_TXN_NONE), LOG);
            try {
                long fileSize = ff.length(fd);
                if (fileSize <= 0) {
                    throw CairoException.critical(ff.errno())
                            .put("timestamp column is too small to attach the partition [path=")
                            .put(path)
                            .put(", fileSize=").put(fileSize)
                            .put(']');
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

    // side effect: sets attachMinTimestamp and attachMaxTimestamp, modifies partitionPath
    // returns partition size
    private long readParquetMinMaxTimestamps(Path filePath, long parquetSize) {
        assert parquetSize > 0;
        long parquetAddr = 0;
        try {
            parquetAddr = mapRO(ff, filePath.$(), LOG, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            parquetDecoder.of(parquetAddr, parquetSize, MemoryTag.NATIVE_TABLE_WRITER);
            final int timestampIndex = metadata.getTimestampIndex();
            parquetColumnIdsAndTypes.clear();
            parquetColumnIdsAndTypes.add(timestampIndex);
            parquetColumnIdsAndTypes.add(timestampType);
            parquetDecoder.readRowGroupStats(parquetStatBuffers, parquetColumnIdsAndTypes, 0);
            attachMinTimestamp = parquetStatBuffers.getMinValueLong(0);
            final int rowGroupCount = parquetDecoder.metadata().getRowGroupCount();
            assert rowGroupCount > 0;
            parquetDecoder.readRowGroupStats(parquetStatBuffers, parquetColumnIdsAndTypes, rowGroupCount - 1);
            attachMaxTimestamp = parquetStatBuffers.getMaxValueLong(0);

            return parquetDecoder.metadata().getRowCount();
        } finally {
            if (parquetAddr != 0) {
                ff.munmap(parquetAddr, parquetSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            }
            Misc.free(parquetDecoder);
        }
    }

    private void readPartitionMinMaxTimestamps(long partitionTimestamp, Path path, CharSequence columnName, long parquetSize, long partitionSize) {
        int partitionLen = path.size();
        try {
            if (ff.exists(path.concat(PARQUET_PARTITION_NAME).$())) {
                readParquetMinMaxTimestamps(path, parquetSize);
            } else {
                path.trimTo(partitionLen);
                readNativeMinMaxTimestamps(path, columnName, partitionSize);
            }
        } finally {
            path.trimTo(partitionLen);
        }

        if (attachMinTimestamp < 0 || attachMaxTimestamp < 0) {
            throw CairoException.critical(ff.errno())
                    .put("cannot read min, max timestamp from the [path=").put(path)
                    .put(", partitionSizeRows=").put(partitionSize)
                    .put(", errno=").put(ff.errno())
                    .put(']');
        }
        if (txWriter.getPartitionTimestampByTimestamp(attachMinTimestamp) != partitionTimestamp
                || txWriter.getPartitionTimestampByTimestamp(attachMaxTimestamp) != partitionTimestamp) {
            throw CairoException.critical(0)
                    .put("invalid timestamp data in detached partition, data does not match partition directory name [path=").put(path)
                    .put(", minTimestamp=").ts(timestampDriver, attachMinTimestamp)
                    .put(", maxTimestamp=").ts(timestampDriver, attachMaxTimestamp)
                    .put(']');
        }
    }

    private long readPartitionSizeMinMaxTimestamps(long partitionTimestamp, Path path, CharSequence columnName, long parquetSize) {
        int partitionLen = path.size();
        try {
            if (ff.exists(path.concat(PARQUET_PARTITION_NAME).$())) {
                return readParquetMinMaxTimestamps(path, parquetSize);
            } else {
                path.trimTo(partitionLen);
                return readNativeSizeMinMaxTimestamps(partitionTimestamp, path, columnName);
            }
        } finally {
            path.trimTo(partitionLen);
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

    private int readTodo(long tableTxn) {
        long todoCount;
        todoCount = openTodoMem(tableTxn);

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
                // use metadata instead of detachedMetadata to get the correct value block capacity
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

    private void recoverFromMetaRenameFailure() {
        openMetaFile(ff, path, pathSize, ddlMem, metadata);
    }

    private void recoverFromSwapRenameFailure() {
        recoverFromTodoWriteFailure();
        clearTodoLog();
    }

    private void recoverFromSymbolMapWriterFailure(CharSequence columnName) {
        removeSymbolMapFilesQuiet(columnName, getTxn());
        removeMetaFile();
        recoverFromSwapRenameFailure();
    }

    private void recoverFromTodoWriteFailure() {
        restoreMetaFrom(META_PREV_FILE_NAME, metaPrevIndex);
        openMetaFile(ff, path, pathSize, ddlMem, metadata);
        columnCount = metadata.getColumnCount();
    }

    private void recoverOpenColumnFailure() {
        removeMetaFile();
        recoverFromSwapRenameFailure();
        // Some writer in-memory state will be still dirty, and it's not easy to roll everything back
        // for all the failure points. It's safer to re-open the writer object after a column-add failure.
        distressed = true;
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
                closeRemove(ff, lockFd, lockName(path));
            } finally {
                path.trimTo(pathSize);
            }
        }
    }

    private ReadOnlyObjList<? extends MemoryCR> remapWalSymbols(
            @Nullable SymbolMapDiffCursor symbolMapDiffCursor,
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
                                // This symbol was not mapped in WAL.
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
                    this.memColumnShifted |= (rowLo != 0);
                }
            }
        }

        if (o3ColumnOverrides.size() == 0) {
            // No mappings were made.
            return o3Columns;
        }
        return o3ColumnOverrides;
    }

    private void removeColumnFiles(int columnIndex, String columnName, int columnType, boolean isIndexed) {
        PurgingOperator purgingOperator = getPurgingOperator();
        if (PartitionBy.isPartitioned(partitionBy)) {
            for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
                long partitionTimestamp = txWriter.getPartitionTimestampByIndex(i);
                if (!txWriter.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp)) {
                    long partitionNameTxn = txWriter.getPartitionNameTxn(i);
                    long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                    purgingOperator.add(
                            columnIndex,
                            columnName,
                            columnType,
                            isIndexed,
                            columnNameTxn,
                            partitionTimestamp,
                            partitionNameTxn
                    );
                }
            }
        } else {
            purgingOperator.add(
                    columnIndex,
                    columnName,
                    columnType,
                    isIndexed,
                    columnVersionWriter.getDefaultColumnNameTxn(columnIndex),
                    txWriter.getLastPartitionTimestamp(),
                    -1
            );
        }
        if (ColumnType.isSymbol(columnType)) {
            purgingOperator.add(
                    columnIndex,
                    columnName,
                    columnType,
                    isIndexed,
                    columnVersionWriter.getSymbolTableNameTxn(columnIndex),
                    PurgingOperator.TABLE_ROOT_PARTITION,
                    -1
            );
        }
    }

    private void removeColumnFilesInPartition(CharSequence columnName, int columnIndex, long partitionTimestamp) {
        if (!txWriter.isPartitionReadOnlyByPartitionTimestamp(partitionTimestamp)) {
            setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, -1);
            int plen = path.size();
            long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
            removeFileOrLog(ff, dFile(path, columnName, columnNameTxn));
            removeFileOrLog(ff, iFile(path.trimTo(plen), columnName, columnNameTxn));
            removeFileOrLog(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn));
            removeFileOrLog(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn));
            path.trimTo(pathSize);
        } else {
            LOG.critical()
                    .$("o3 ignoring removal of column in read-only partition [table=").$(tableToken)
                    .$(", column=").$safe(columnName)
                    .$(", timestamp=").$ts(timestampDriver, partitionTimestamp)
                    .$();
        }
    }

    private void removeIndexFilesInPartition(CharSequence columnName, int columnIndex, long partitionTimestamp, long partitionNameTxn) {
        setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, partitionNameTxn);
        int plen = path.size();
        long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        removeFileOrLog(ff, keyFileName(path.trimTo(plen), columnName, columnNameTxn));
        removeFileOrLog(ff, valueFileName(path.trimTo(plen), columnName, columnNameTxn));
        path.trimTo(pathSize);
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
                long dirTimestamp = partitionDirFmt.parse(utf8Sink.asAsciiCharSequence(), 0, txnSep, EN_LOCALE);
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

    private void removeSymbolMapFilesQuiet(CharSequence name, long columnNameTxn) {
        try {
            removeFileOrLog(ff, offsetFileName(path.trimTo(pathSize), name, columnNameTxn));
            removeFileOrLog(ff, charFileName(path.trimTo(pathSize), name, columnNameTxn));
            removeFileOrLog(ff, keyFileName(path.trimTo(pathSize), name, columnNameTxn));
            removeFileOrLog(ff, valueFileName(path.trimTo(pathSize), name, columnNameTxn));
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

    private void renameMetaToMetaPrev() {
        try {
            this.metaPrevIndex = rename(fileOperationRetryCount);
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, e);
        }
    }

    private void renameSwapMetaToMeta() {
        // rename _meta.swp to _meta
        try {
            restoreMetaFrom(META_SWAP_FILE_NAME, metaSwapIndex);
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_SWAP_RENAME_FAILURE, e);
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
                        oldPartitionName(other, getTxn());
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
                        oldPartitionName(other, getTxn());
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
                            dFile(path.trimTo(p), metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE);
                            maxTimestamp = readLongAtOffset(ff, path.$(), tempMem16b, (transientRowCount - 1) * Long.BYTES);
                            fixedRowCount -= transientRowCount;
                            txWriter.removeAttachedPartitions(txWriter.getMaxTimestamp());
                            LOG.info()
                                    .$("updated active partition [name=").$(path.trimTo(p).$())
                                    .$(", maxTimestamp=").$ts(timestampDriver, maxTimestamp)
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
                        .$("actual table size has been adjusted [name=`").$(tableToken).$('`')
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

    private void repairMetaRename(MemoryMARW todoMem) {
        try {
            if (todoMem.size() < TODO_META_INDEX_OFFSET + Long.BYTES) {
                throw CairoException.critical(0).put("cannot restore metadata, todo file is too short, may be corrupt at ").put(path.concat(TODO_FILE_NAME));
            }
            int index = (int) todoMem.getLong(TODO_META_INDEX_OFFSET);

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

            renameOrFail(ff, path.$(), other.concat(META_FILE_NAME).$());
        } finally {
            path.trimTo(pathSize);
            other.trimTo(pathSize);
        }
    }

    private void rewriteAndSwapMetadata(TableWriterMetadata metadata) {
        // create new _meta.swp
        this.metaSwapIndex = rewriteMetadata(metadata);

        // validate new meta
        validateSwapMeta();

        // rename _meta to _meta.prev
        renameMetaToMetaPrev();

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo();

        // rename _meta.swp to _meta
        renameSwapMetaToMeta();
    }

    private int rewriteMetadata(TableWriterMetadata metadata) {
        try {
            int columnCount = metadata.getColumnCount();
            int index = openMetaSwapFile(ff, ddlMem, path, pathSize, configuration.getMaxSwapFileCount());

            ddlMem.putInt(metadata.getColumnCount());
            ddlMem.putInt(metadata.getPartitionBy());
            ddlMem.putInt(metadata.getTimestampIndex());
            ddlMem.putInt(ColumnType.VERSION);
            ddlMem.putInt(metadata.getTableId());
            ddlMem.putInt(metadata.getMaxUncommittedRows());
            ddlMem.putLong(metadata.getO3MaxLag());

            int version = txWriter.getMetadataVersion() + 1;
            metadata.setMetadataVersion(version);
            ddlMem.putLong(metadata.getMetadataVersion());
            ddlMem.putBool(metadata.isWalEnabled());
            ddlMem.putInt(TableUtils.calculateMetaFormatMinorVersionField(version, columnCount));
            ddlMem.putInt(metadata.getTtlHoursOrMonths());

            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                int columnType = metadata.getColumnType(i);
                ddlMem.putInt(columnType);

                long flags = 0;
                if (metadata.isIndexed(i)) {
                    flags |= META_FLAG_BIT_INDEXED;
                }

                if (metadata.isDedupKey(i)) {
                    flags |= META_FLAG_BIT_DEDUP_KEY;
                }

                if (metadata.getSymbolCacheFlag(i)) {
                    flags |= META_FLAG_BIT_SYMBOL_CACHE;
                }

                ddlMem.putLong(flags);

                ddlMem.putInt(metadata.getIndexBlockCapacity(i));
                ddlMem.putInt(metadata.getSymbolCapacity(i));
                ddlMem.skip(4);
                int replaceColumnIndex = metadata.getReplacingColumnIndex(i);
                ddlMem.putInt(replaceColumnIndex > -1 ? replaceColumnIndex + 1 : 0);
                ddlMem.skip(4);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            ddlMem.jumpTo(nameOffset);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metadata.getColumnName(i);
                ddlMem.putStr(columnName);
                nameOffset += Vm.getStorageLength(columnName);
            }

            ddlMem.sync(false);
            return index;
        } catch (Throwable th) {
            LOG.critical().$("could not write to metadata file, rolling back DDL [path=").$(path).$(']').$();
            try {
                // Revert metadata
                openMetaFile(ff, path, pathSize, ddlMem, metadata);
            } catch (Throwable th2) {
                LOG.critical().$("could not revert metadata, writer distressed [path=").$(path).$(']').$();
                throwDistressException(th2);
            }
            throw th;
        } finally {
            // truncate _meta file exactly, the file size never changes.
            // Metadata updates are written to a new file and then swapped by renaming.
            ddlMem.close(true, Vm.TRUNCATE_TO_POINTER);
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

    private void rollbackRemoveIndexFiles(CharSequence columnName, int columnIndex) {
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

    private void rollbackSymbolTables(boolean quiet) {
        int expectedMapWriters = txWriter.unsafeReadSymbolColumnCount();
        for (int i = 0; i < expectedMapWriters; i++) {
            try {
                denseSymbolMapWriters.get(i).rollback(txWriter.unsafeReadSymbolWriterIndexOffset(i));
            } catch (Throwable th) {
                if (quiet) {
                    distressed = true;
                    CharSequence columnName = metadata.getColumnName(i);
                    LOG.error().$("could not rollback symbol table [table=").$(tableToken)
                            .$(", columnName=").$safe(columnName)
                            .$(", exception=").$(th)
                            .I$();
                } else {
                    throw th;
                }
            }
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

    private void runFragile(FragileCode fragile, CairoException e) {
        try {
            fragile.run();
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

    private void scaleSymbolCapacities() {
        if (configuration.autoScaleSymbolCapacity()) {
            for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
                var w = denseSymbolMapWriters.getQuick(i);
                var columnIndex = w.getColumnIndex();
                // ignore null writers, if they are present in the list
                if (columnIndex > -1) {
                    int symbolCount = w.getSymbolCount();
                    int symbolCapacity = w.getSymbolCapacity();
                    if (symbolCount > symbolCapacity * configuration.autoScaleSymbolCapacityThreshold()) {
                        changeSymbolCapacity(
                                metadata.getColumnName(w.getColumnIndex()),
                                // scale capacity to symbol count, if that grows rapidly, and to capacity increments if
                                // symbol count grows slowly
                                Math.max(symbolCapacity * 2, Numbers.floorPow2(symbolCount)),
                                AllowAllSecurityContext.INSTANCE // this is internal housekeeping task, triggered by WAL apply job
                        );
                    }
                }
            }
        }
    }

    private void scheduleRemoveAllPartitions() {
        for (int i = txWriter.getPartitionCount() - 1; i > -1L; i--) {
            long timestamp = txWriter.getPartitionTimestampByIndex(i);
            long partitionTxn = txWriter.getPartitionNameTxn(i);
            partitionRemoveCandidates.add(timestamp, partitionTxn);
        }
    }

    private void setAppendPosition(final long rowCount, boolean doubleAllocate) {
        engine.getPartitionOverwriteControl().notifyPartitionMutates(
                tableToken,
                timestampType,
                lastOpenPartitionTs,
                lastOpenPartitionTxnName,
                rowCount
        );

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
     * partitionLo and partitionHi to the partition interval in millis. These values are
     * determined based on input timestamp and value of partitionBy. For any given
     * timestamp, this method will determine either day, month or year interval timestamp falls to.
     * Partition directory name is ISO string of interval start.
     * <p>
     * Because this method modifies "path" member variable, be a sure path trimmed to original
     * state within try..finally block.
     *
     * @param path      path instance to modify
     * @param timestamp to determine the interval for
     */
    private long setStateForTimestamp(Path path, long timestamp) {
        // When partition is created, a txn name must always be set to purge dropped partitions.
        // When partition is created outside O3 merge use `txn-1` as the version
        long partitionTxnName = PartitionBy.isPartitioned(partitionBy) ? txWriter.getTxn() - 1 : -1;
        partitionTxnName = txWriter.getPartitionNameTxnByPartitionTimestamp(timestamp, partitionTxnName);
        setPathForNativePartition(path, timestampType, partitionBy, timestamp, partitionTxnName);
        return partitionTxnName;
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
        if (lastLogicalPartitionTimestamp != txWriter.getLogicalPartitionTimestamp(lastLogicalPartitionTimestamp)) {
            lastLogicalPartitionTimestamp = txWriter.getLogicalPartitionTimestamp(lastLogicalPartitionTimestamp);
            // We can have a split partition without the parent logical partition
            // after some replace commits
            // We need to create a logical partition to squash into
            txWriter.insertPartition(partitionIndex, lastLogicalPartitionTimestamp, 0, txWriter.txn);
            setStateForTimestamp(other, lastLogicalPartitionTimestamp);
            if (ff.mkdir(other.$(), configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(ff.errno()).put("could not create directory [path='").put(other).put("']");
            }
            partitionIndex++;
        }

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

    private void squashSplitPartitions(long timestampMin, long timestampMax, int maxLastSubPartitionCount) {
        if (timestampMin > txWriter.getMaxTimestamp() || txWriter.getPartitionCount() < 2) {
            return;
        }

        // Take control of the split partition population here.
        // When the number of split partitions is too big, start merging them together.
        // This is to avoid having too many partitions / files in the system which penalizes the reading performance.
        long logicalPartitionTimestamp = txWriter.getLogicalPartitionTimestamp(timestampMin);
        int partitionIndexLo = squashSplitPartitions_findPartitionIndexAtOrGreaterTimestamp(logicalPartitionTimestamp);

        boolean splitsKept = false;
        if (partitionIndexLo < txWriter.getPartitionCount()) {
            int partitionIndexHi = Math.min(squashSplitPartitions_findPartitionIndexAtOrGreaterTimestamp(timestampMax) + 1, txWriter.getPartitionCount());
            int partitionIndex = partitionIndexLo + 1;

            for (; partitionIndex < partitionIndexHi; partitionIndex++) {

                long nextPartitionTimestamp = txWriter.getPartitionTimestampByIndex(partitionIndex);
                long nextPartitionLogicalTimestamp = txWriter.getLogicalPartitionTimestamp(nextPartitionTimestamp);

                if (nextPartitionLogicalTimestamp != logicalPartitionTimestamp) {
                    int splitCount = partitionIndex - partitionIndexLo;
                    if (splitCount > 1) {
                        int partitionCount = txWriter.getPartitionCount();
                        squashPartitionRange(maxLastSubPartitionCount, partitionIndexLo, partitionIndex);
                        int partitionReduction = partitionCount - txWriter.getPartitionCount();
                        splitsKept = partitionReduction < splitCount - 1;

                        logicalPartitionTimestamp = nextPartitionTimestamp;

                        // Squashing changes the partitions, re-calculate the loop index and boundaries
                        partitionIndexLo = partitionIndex = squashSplitPartitions_findPartitionIndexAtOrGreaterTimestamp(nextPartitionTimestamp);
                        partitionIndexHi = Math.min(squashSplitPartitions_findPartitionIndexAtOrGreaterTimestamp(timestampMax) + 1, txWriter.getPartitionCount());
                    } else {
                        partitionIndexLo = partitionIndex;
                        logicalPartitionTimestamp = nextPartitionLogicalTimestamp;
                    }

                    if (!splitsKept && timestampMin == minSplitPartitionTimestamp) {
                        // All splits seen are squashed, move minSplitPartitionTimestamp forward.
                        minSplitPartitionTimestamp = nextPartitionTimestamp;
                    }
                }
            }

            if (partitionIndex - partitionIndexLo > 1) {
                squashPartitionRange(maxLastSubPartitionCount, partitionIndexLo, partitionIndex);
            }
        }
    }

    private void squashSplitPartitions(final int partitionIndexLo, final int partitionIndexHi, final int optimalPartitionCount, boolean force) {
        if (partitionIndexHi <= partitionIndexLo + Math.max(1, optimalPartitionCount)) {
            // Nothing to do
            return;
        }

        if (checkpointStatus.partitionsLocked()) {
            LOG.info().$("cannot squash partition [table=").$(tableToken)
                    .$("], checkpoint in progress").$();
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
        setPathForNativePartition(path, timestampType, partitionBy, targetPartition, targetPartitionNameTxn);
        final long originalSize = txWriter.getPartitionRowCountByTimestamp(targetPartition);

        boolean rw = !copyTargetFrame;
        Frame targetFrame = null;
        FrameFactory frameFactory = engine.getFrameFactory();
        Frame firstPartitionFrame = frameFactory.open(rw, path, targetPartition, metadata, columnVersionWriter, originalSize);
        try {
            if (copyTargetFrame) {
                try {
                    setPathForNativePartition(other, timestampType, partitionBy, targetPartition, txWriter.txn);
                    createDirsOrFail(ff, other, configuration.getMkDirMode());
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
                    timestampType,
                    targetPartition,
                    targetPartitionNameTxn,
                    targetFrame.getRowCount()
            );
            for (int i = 0; i < squashCount; i++) {
                long sourcePartition = txWriter.getPartitionTimestampByIndex(targetPartitionIndex + 1);

                other.trimTo(pathSize);
                long sourceNameTxn = txWriter.getPartitionNameTxnByPartitionTimestamp(sourcePartition);
                setPathForNativePartition(other, timestampType, partitionBy, sourcePartition, sourceNameTxn);
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
                    LOG.critical().$("partition squashing failed [table=").$(tableToken)
                            .$(", error=").$(th).I$();
                    throw th;
                }

                txWriter.removeAttachedPartitions(sourcePartition);
                columnVersionWriter.squashPartition(targetPartition, sourcePartition);
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

        if (lastPartitionSquashed) {
            openLastPartition();
        }

        // Commit the new transaction with the partitions squashed
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.commit(denseSymbolMapWriters);
    }

    private int squashSplitPartitions_findPartitionIndexAtOrGreaterTimestamp(long timestampMax) {
        int partitionIndex = txWriter.findAttachedPartitionIndexByLoTimestamp(timestampMax);
        if (partitionIndex < 0) {
            return -partitionIndex - 1;
        }
        return partitionIndex;
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

    private long swapTimestampInMemCols(int timestampIndex) {
        long timestampAddr;
        var tsMem1 = o3MemColumns1.get(getPrimaryColumnIndex(timestampIndex));
        var tsMem2 = o3MemColumns2.get(getPrimaryColumnIndex(timestampIndex));

        o3MemColumns1.set(getPrimaryColumnIndex(timestampIndex), tsMem2);
        o3TimestampMem = tsMem2;

        o3MemColumns2.set(getPrimaryColumnIndex(timestampIndex), tsMem1);
        o3TimestampMemCpy = tsMem1;

        timestampAddr = o3TimestampMem.getAddress();
        return timestampAddr;
    }

    private void switchPartition(long timestamp) {
        // Before partition can be switched, we need to index records
        // added so far. Index writers will start point to different
        // files after switch.
        updateIndexes();
        txWriter.switchPartitions(timestamp);
        openPartition(timestamp, 0);
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

    private void throwApplyBlockColumnShuffleFailed(int columnIndex, int columnType, long totalRows, long rowCount) {
        LOG.error().$("wal block apply failed [table=").$(tableToken)
                .$(", column=").$safe(metadata.getColumnName(columnIndex))
                .$(", columnType=").$(ColumnType.nameOf(columnType))
                .$(", expectedResult=").$(totalRows)
                .$(", actualResult=").$(rowCount)
                .I$();

        if (configuration.getDebugWalApplyBlockFailureNoRetry()) {
            // This exception is thrown to indicate that the applying block failed,
            // and it is not of CairoException type so that it is not intercepted and re-tried in tests.
            // The purpose is to have the special debug test mode where this exception
            // suspends the table and fails the testing code instead of switching to 1 by 1 commit mode.
            throw new IllegalStateException("apply block failed");
        }
        throw CairoException.txnApplyBlockError(tableToken);
    }

    private void throwDistressException(Throwable cause) {
        try {
            Sinkable sinkable = (Sinkable) cause;
            LOG.critical().$("writer error [table=").$(tableToken).$(", e=").$(sinkable).I$();
        } catch (Throwable th) {
            LOG.critical().$("writer error [table=").$(tableToken).$(", e=").$(cause).I$();
        }
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
        clearTodoLog();
        this.minSplitPartitionTimestamp = Long.MAX_VALUE;
        processPartitionRemoveCandidates();

        LOG.info().$("truncated [name=").$(tableToken).I$();

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
                if (metadata.isIndexed(i)) {
                    // Reset indexer column top
                    indexers.get(i).resetColumnTop();
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

        LOG.info().$("parallel indexing [table=").$(tableToken)
                .$(", indexCount=").$(indexCount)
                .$(", rowCount=").$(hi - lo)
                .I$();
        int serialIndexCount = 0;

        // we are going to index the last column in this thread while other columns are on the queue
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

        // At this point, we have re-indexed our column and if things are flowing nicely
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
        LOG.debug().$("serial indexing [table=").$(tableToken)
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
        LOG.debug().$("serial indexing done [table=").$(tableToken).I$();
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

    private void updateMatViewDefinition(MatViewDefinition newDefinition) {
        if (blockFileWriter == null) {
            blockFileWriter = new BlockFileWriter(ff, configuration.getCommitMode());
        }
        try (BlockFileWriter definitionWriter = blockFileWriter) {
            definitionWriter.of(path.concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());
            MatViewDefinition.append(newDefinition, definitionWriter);
        } finally {
            path.trimTo(pathSize);
        }

        // Unlike mat view state write-through behavior, we update the in-memory definition
        // object here, after updating the definition file.
        engine.getMatViewGraph().updateViewDefinition(tableToken, newDefinition);
        engine.getMatViewStateStore().updateViewDefinition(tableToken, newDefinition);
    }

    private void updateMaxTimestamp(long timestamp) {
        txWriter.updateMaxTimestamp(timestamp);
        this.timestampSetter.accept(timestamp);
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

    private void validateSwapMeta() {
        try {
            try {
                path.concat(META_SWAP_FILE_NAME);
                if (metaSwapIndex > 0) {
                    path.put('.').put(metaSwapIndex);
                }
                // Map meta-swap file for verification to exact length.
                long len = ff.length(path.$());
                // Check that file length is ok, do not allow you to map to default page size if it's returned as -1.
                if (len < 1) {
                    throw CairoException.critical(ff.errno()).put("cannot swap metadata file, invalid size: ").put(path);
                }
                ddlMem.of(ff, path.$(), ff.getPageSize(), len, MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE, POSIX_MADV_RANDOM);
                validationMap.clear();
                validateMeta(path, ddlMem, validationMap, ColumnType.VERSION);
            } finally {
                ddlMem.close(false);
                path.trimTo(pathSize);
            }
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, e);
        }
    }

    private void writeIndex(@NotNull CharSequence columnName, int indexValueBlockSize, int columnIndex, SymbolColumnIndexer indexer) {
        // create indexer
        final long columnNameTxn = columnVersionWriter.getColumnNameTxn(txWriter.getLastPartitionTimestamp(), columnIndex);
        try {
            try {
                // edge cases here are:
                // column spans only part of table - e.g., it was added after table was created and populated
                // column has top value, e.g., does not span the entire partition
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
        } catch (Throwable th) {
            Misc.free(indexer);
            LOG.error().$("rolling back index created so far [path=").$substr(pathRootSize, path).I$();
            rollbackRemoveIndexFiles(columnName, columnIndex);
            throw th;
        }
    }

    private void writeMetadataToDisk() {
        rewriteAndSwapMetadata(metadata);
        clearTodoAndCommitMeta();
        try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
            metadataRW.hydrateTable(metadata);
        }
    }

    private void writeRestoreMetaTodo() {
        try {
            todoMem.putLong(0, txWriter.txn); // write txn, reader will first read txn at offset 24 and then at offset 0
            Unsafe.getUnsafe().storeFence(); // make sure we do not write hash before writing txn (view from another thread)
            todoMem.putLong(8, configuration.getDatabaseIdLo()); // write out our instance hashes
            todoMem.putLong(16, configuration.getDatabaseIdHi());
            Unsafe.getUnsafe().storeFence();
            todoMem.putLong(32, 1);
            todoMem.putLong(40, TODO_RESTORE_META);
            todoMem.putLong(48, metaPrevIndex);
            Unsafe.getUnsafe().storeFence();
            todoMem.putLong(24, txWriter.txn);
            todoMem.jumpTo(56);
            todoMem.sync(false);
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_TODO_WRITE_FAILURE, e);
        }
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

    /**
     * This method is thread safe, e.g. can be triggered from multiple partition merge tasks
     *
     * @param partitionTimestamp timestamp of the partition to check
     * @param partitionNameTxn   transaction name of the partition to check
     * @param partitionRowCount  row count of the partition to check
     * @param partitionLo        starting row index of the partition to check
     * @param partitionHi        ending row index of the partition to check, inclusive
     * @param commitLo           starting row index of the commit to check
     * @param commitHi           ending row index of the commit to check, inclusive
     * @param mergeIndexAddr     address of the merge index
     * @param mergeIndexRows     number of rows in the merge index
     * @return true if the commit is identical to the partition, false otherwise
     */
    boolean checkDedupCommitIdenticalToPartition(
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionRowCount,
            long partitionLo,
            long partitionHi,
            long commitLo,
            long commitHi,
            long mergeIndexAddr,
            long mergeIndexRows
    ) {
        TableRecordMetadata metadata = getMetadata();
        FrameFactory frameFactory = engine.getFrameFactory();
        try (Frame partitionFrame = frameFactory.openRO(path, partitionTimestamp, partitionNameTxn, partitionBy, metadata, columnVersionWriter, partitionRowCount)) {
            try (Frame commitFrame = engine.getFrameFactory().openROFromMemoryColumns(o3Columns, this.metadata, commitRowCount)) {
                for (int i = 0; i < metadata.getColumnCount(); i++) {
                    // Do not compare dedup keys, already a match
                    if (!metadata.isDedupKey(i) && !FrameAlgebra.isColumnReplaceIdentical(
                            i,
                            partitionFrame,
                            partitionLo,
                            partitionHi + 1,
                            commitFrame,
                            commitLo,
                            commitHi + 1,
                            mergeIndexAddr,
                            mergeIndexRows
                    )) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * This method is thread safe, e.g. can be triggered from multiple partition merge tasks
     *
     * @param partitionTimestamp timestamp of the partition to check
     * @param partitionNameTxn   transaction name of the partition to check
     * @param partitionRowCount  row count of the partition to check
     * @param partitionLo        starting row index of the partition to check
     * @param partitionHi        ending row index of the partition to check, inclusive
     * @param commitLo           starting row index of the commit to check
     * @param commitHi           ending row index of the commit to check, inclusive
     * @return true if the commit is identical to the partition, false otherwise
     */
    boolean checkReplaceCommitIdenticalToPartition(
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionRowCount,
            long partitionLo,
            long partitionHi,
            long commitLo,
            long commitHi
    ) {
        TableRecordMetadata metadata = getMetadata();
        FrameFactory frameFactory = engine.getFrameFactory();
        try (Frame partitionFrame = frameFactory.openRO(path, partitionTimestamp, partitionNameTxn, partitionBy, metadata, columnVersionWriter, partitionRowCount)) {
            try (Frame commitFrame = engine.getFrameFactory().openROFromMemoryColumns(o3Columns, this.metadata, commitRowCount)) {
                for (int i = 0; i < metadata.getColumnCount(); i++) {

                    // Compare all columns, dedup keys and non-keys
                    if (i != metadata.getTimestampIndex()) {
                        // Non-designated timestamp
                        if (!FrameAlgebra.isColumnReplaceIdentical(
                                i,
                                partitionFrame,
                                partitionLo,
                                partitionHi + 1,
                                commitFrame,
                                commitLo,
                                commitHi + 1,
                                0,
                                commitHi + 1 - commitLo
                        )) {
                            return false;
                        }
                    } else {
                        // Designated timestamp
                        if (!FrameAlgebra.isDesignatedTimestampColumnReplaceIdentical(
                                i,
                                partitionFrame,
                                partitionLo,
                                partitionHi + 1,
                                commitFrame,
                                commitLo,
                                commitHi + 1
                        )) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
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
                // Cancelling the first row in o3, reverting to non-o3
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
                // we have to undo the creation of partition
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
                        openPartition(rollbackToMaxTimestamp, rollbackToTransientRowCount);
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
            // we are staying within the same partition, prepare append positions for row count
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

            // if no column has been changed we take easy option and do nothing
            if (rowChanged) {
                setAppendPosition(dirtyTransientRowCount - 1, false);
            }
        }
        rowValueIsNotNull.fill(0, columnCount, --masterRef);

        // Some execution path in this method already call txWriter.removeAllPartitions()
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
        void run();
    }

    public interface Row {

        void append();

        void cancel();

        void putArray(int columnIndex, @NotNull ArrayView array);

        void putBin(int columnIndex, long address, long len);

        void putBin(int columnIndex, BinarySequence sequence);

        void putBool(int columnIndex, boolean value);

        void putByte(int columnIndex, byte value);

        void putChar(int columnIndex, char value);

        void putDate(int columnIndex, long value);

        void putDecimal(int columnIndex, Decimal256 value);

        void putDecimal128(int columnIndex, long high, long low);

        void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll);

        void putDecimalStr(int columnIndex, CharSequence decimalValue);

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
         * Writes UTF8-encoded string to WAL. As the name of the function suggests the storage format is
         * expected to be UTF16. The function must re-encode string from UTF8 to UTF16 before storing.
         *
         * @param columnIndex index of the column we are writing to
         * @param value       UTF8 bytes represented as CharSequence interface.
         *                    On this interface, getChar() returns a byte, not complete character.
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

        @SuppressWarnings("unused")
            // Used by assembler
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
        public void putArray(int columnIndex, @NotNull ArrayView array) {
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
        public void putDecimal(int columnIndex, Decimal256 value) {
            // no-op
        }

        @Override
        public void putDecimal128(int columnIndex, long high, long low) {
            // no-op
        }

        @Override
        public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
            // no-op
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
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
        private final Decimal256 decimal256Sink = new Decimal256();

        @Override
        public void append() {
            rowAppend(activeNullSetters);
        }

        @Override
        public void cancel() {
            rowCancel();
        }

        @Override
        public void putArray(int columnIndex, @NotNull ArrayView array) {
            ArrayTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex),
                    getPrimaryColumn(columnIndex),
                    array
            );
            setRowValueNotNull(columnIndex);
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
        public void putDecimal(int columnIndex, Decimal256 value) {
            int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putDecimal(columnIndex, value, type, this);
        }

        @Override
        public void putDecimal128(int columnIndex, long high, long low) {
            getPrimaryColumn(columnIndex).putDecimal128(high, low);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDecimal256(int columnIndex, long hh, long hl, long lh, long ll) {
            getPrimaryColumn(columnIndex).putDecimal256(hh, hl, lh, ll);
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putDecimalStr(int columnIndex, CharSequence decimalValue) {
            final int type = metadata.getColumnType(columnIndex);
            WriterRowUtils.putDecimalStr(columnIndex, decimal256Sink, decimalValue, type, this);
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
                    getSecondaryColumn(columnIndex),
                    getPrimaryColumn(columnIndex),
                    utf8Sink
            );
            setRowValueNotNull(columnIndex);
        }

        @Override
        public void putVarchar(int columnIndex, Utf8Sequence value) {
            VarcharTypeDriver.appendValue(
                    getSecondaryColumn(columnIndex),
                    getPrimaryColumn(columnIndex),
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
