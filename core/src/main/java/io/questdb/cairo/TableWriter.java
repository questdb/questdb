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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.*;
import io.questdb.std.microtime.TimestampFormat;
import io.questdb.std.microtime.TimestampFormatUtils;
import io.questdb.std.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import io.questdb.tasks.ColumnIndexerTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.util.function.LongConsumer;

import static io.questdb.cairo.TableUtils.*;

public class TableWriter implements Closeable {

    public static final int TIMESTAMP_MERGE_ENTRY_BYTES = Long.BYTES * 2;
    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final CharSequenceHashSet IGNORED_FILES = new CharSequenceHashSet();
    private static final Runnable NOOP = () -> {
    };
    private final static RemoveFileLambda REMOVE_OR_LOG = TableWriter::removeFileAndOrLog;
    private final static RemoveFileLambda REMOVE_OR_EXCEPTION = TableWriter::removeOrException;
    private final static ShuffleOutOfOrderDataInternal SHUFFLE_8 = Vect::indexReshuffle8Bit;
    private final static ShuffleOutOfOrderDataInternal SHUFFLE_16 = Vect::indexReshuffle16Bit;
    private final static ShuffleOutOfOrderDataInternal SHUFFLE_32 = Vect::indexReshuffle32Bit;
    private final static ShuffleOutOfOrderDataInternal SHUFFLE_64 = Vect::indexReshuffle64Bit;
    private final static MergeShuffleOutOfOrderDataInternal MERGE_SHUFFLE_8 = Vect::mergeShuffle8Bit;
    private final static MergeShuffleOutOfOrderDataInternal MERGE_SHUFFLE_16 = Vect::mergeShuffle16Bit;
    private final static MergeShuffleOutOfOrderDataInternal MERGE_SHUFFLE_32 = Vect::mergeShuffle32Bit;
    private final static MergeShuffleOutOfOrderDataInternal MERGE_SHUFFLE_64 = Vect::mergeShuffle64Bit;
    private static final int OO_BLOCK_NONE = -1;
    private static final int OO_BLOCK_OO = 1;
    private static final int OO_BLOCK_DATA = 2;
    private static final int OO_BLOCK_MERGE = 3;
    final ObjList<AppendMemory> columns;
    private final ObjList<SymbolMapWriter> symbolMapWriters;
    private final ObjList<SymbolMapWriter> denseSymbolMapWriters;
    private final ObjList<ColumnIndexer> indexers;
    private final ObjList<ColumnIndexer> denseIndexers = new ObjList<>();
    private final Path path;
    private final Path other;
    private final LongList refs = new LongList();
    private final Row row = new Row();
    private final int rootLen;
    private final ReadWriteMemory txMem;
    private final ReadOnlyMemory metaMem;
    private final ContiguousVirtualMemory txPendingPartitionSizes;
    private final int partitionBy;
    private final RowFunction switchPartitionFunction = new SwitchPartitionRowFunction();
    private final RowFunction openPartitionFunction = new OpenPartitionRowFunction();
    private final RowFunction noPartitionFunction = new NoPartitionFunction();
    private final RowFunction mergePartitionFunction = new MergePartitionFunction();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final LongList columnTops;
    private final FilesFacade ff;
    private final TimestampFormat partitionDirFmt;
    private final AppendMemory ddlMem;
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
    private final LongHashSet removedPartitions = new LongHashSet();
    private final Timestamps.TimestampFloorMethod timestampFloorMethod;
    private final Timestamps.TimestampAddMethod timestampAddMethod;
    private final int defaultCommitMode;
    private final FindVisitor removePartitionDirectories = this::removePartitionDirectories0;
    private final ObjList<Runnable> nullers;
    private final ObjList<ContiguousVirtualMemory> mergeColumns;
    private final OnePageMemory timestampSearchColumn = new OnePageMemory();
    private ContiguousVirtualMemory tmpShuffleIndex;
    private ContiguousVirtualMemory tmpShuffleData;
    private ContiguousVirtualMemory timestampMergeMem;
    private int txPartitionCount = 0;
    private long lockFd;
    private LongConsumer timestampSetter;
    private int columnCount;
    private long fixedRowCount = 0;
    private long txn;
    private long structureVersion;
    private long dataVersion;
    private RowFunction rowFunction = openPartitionFunction;
    private long prevMaxTimestamp;
    private long txPrevTransientRowCount;
    private long maxTimestamp;
    private long minTimestamp;
    private long prevMinTimestamp;
    private long partitionHi;
    private long transientRowCount = 0;
    private long masterRef = 0;
    private boolean removeDirOnCancelRow = true;
    private long tempMem8b = Unsafe.malloc(8);
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
    private long mergeRowCount;
    private final LongConsumer mergeTimestampMethodRef = this::mergeTimestampSetter;
    private long transientRowCountBeforeOutOfOrder;

    public TableWriter(CairoConfiguration configuration, CharSequence name) {
        this(configuration, name, null);
    }

    public TableWriter(CairoConfiguration configuration, CharSequence name, MessageBus messageBus) {
        this(configuration, name, messageBus, true, DefaultLifecycleManager.INSTANCE);
    }

    public TableWriter(
            CairoConfiguration configuration,
            CharSequence name,
            @Nullable MessageBus messageBus,
            boolean lock,
            LifecycleManager lifecycleManager
    ) {
        this(configuration, name, messageBus, lock, lifecycleManager, configuration.getRoot());
    }

    public TableWriter(
            CairoConfiguration configuration,
            CharSequence name,
            @Nullable MessageBus messageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            CharSequence root
    ) {
        LOG.info().$("open '").utf8(name).$('\'').$();
        this.configuration = configuration;
        this.messageBus = messageBus;
        this.defaultCommitMode = configuration.getCommitMode();
        this.lifecycleManager = lifecycleManager;
        this.parallelIndexerEnabled = messageBus != null && configuration.isParallelIndexingEnabled();
        this.ff = configuration.getFilesFacade();
        this.mkDirMode = configuration.getMkDirMode();
        this.fileOperationRetryCount = configuration.getFileOperationRetryCount();
        this.path = new Path().of(root).concat(name);
        this.other = new Path().of(root).concat(name);
        this.name = Chars.toString(name);
        this.rootLen = path.length();
        try {
            if (lock) {
                lock();
            } else {
                this.lockFd = -1L;
            }
            this.txMem = openTxnFile();
            long todo = readTodoTaskCode();
            if (todo != -1L && (int) (todo & 0xff) == TODO_RESTORE_META) {
                repairMetaRename((int) (todo >> 8));
            }
            this.ddlMem = new AppendMemory();
            this.metaMem = new ReadOnlyMemory();
            openMetaFile();
            this.metadata = new TableWriterMetadata(ff, metaMem);

            // we have to do truncate repair at this stage of constructor
            // because this operation requires metadata
            if (todo != -1L) {
                switch ((int) (todo & 0xff)) {
                    case TODO_TRUNCATE:
                        repairTruncate();
                        break;
                    case TODO_RESTORE_META:
                        break;
                    default:
                        LOG.error().$("ignoring unknown *todo* [code=").$(todo).$(']').$();
                        break;
                }
            }
            this.columnCount = metadata.getColumnCount();
            this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
            this.txPendingPartitionSizes = new ContiguousVirtualMemory(ff.getPageSize(), Integer.MAX_VALUE);
            this.tmpShuffleIndex = new ContiguousVirtualMemory(ff.getPageSize(), Integer.MAX_VALUE);
            this.tmpShuffleData = new ContiguousVirtualMemory(ff.getPageSize(), Integer.MAX_VALUE);
            this.refs.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount * 2);
            this.mergeColumns = new ObjList<>(columnCount * 2);
            this.row.rowColumns = columns;
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.indexers = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(metadata.getSymbolMapCount());
            this.nullers = new ObjList<>(columnCount);
            this.columnTops = new LongList(columnCount);
            switch (partitionBy) {
                case PartitionBy.DAY:
                    timestampFloorMethod = Timestamps.FLOOR_DD;
                    timestampAddMethod = Timestamps.ADD_DD;
                    partitionDirFmt = fmtDay;
                    break;
                case PartitionBy.MONTH:
                    timestampFloorMethod = Timestamps.FLOOR_MM;
                    timestampAddMethod = Timestamps.ADD_MM;
                    partitionDirFmt = fmtMonth;
                    break;
                case PartitionBy.YEAR:
                    // year
                    timestampFloorMethod = Timestamps.FLOOR_YYYY;
                    timestampAddMethod = Timestamps.ADD_YYYY;
                    partitionDirFmt = fmtYear;
                    break;
                default:
                    timestampFloorMethod = null;
                    timestampAddMethod = null;
                    partitionDirFmt = null;
                    break;
            }

            configureColumnMemory();
            timestampSetter = configureTimestampSetter();
            configureAppendPosition();
            purgeUnusedPartitions();
            loadRemovedPartitions();
        } catch (CairoException e) {
            LOG.error().$("cannot open '").$(path).$("' and this is why: {").$((Sinkable) e).$('}').$();
            doClose(false);
            throw e;
        }
    }

    public static TimestampFormat selectPartitionDirFmt(int partitionBy) {
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
        columnTops.extendAndSet(columnCount - 1, transientRowCount);

        // create column files
        if (transientRowCount > 0 || partitionBy == PartitionBy.NONE) {
            try {
                openNewColumnFiles(name, isIndexed, indexValueBlockCapacity);
            } catch (CairoException e) {
                runFragile(RECOVER_FROM_COLUMN_OPEN_FAILURE, name, e);
            }
        }

        try {
            // open _meta file
            openMetaFile();

            // remove _todo
            removeTodoFile();

        } catch (CairoException err) {
            throwDistressException(err);
        }

        bumpStructureVersion();

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
            openMetaFile();

            // remove _todo
            removeTodoFile();

        } catch (CairoException err) {
            throwDistressException(err);
        }

        bumpStructureVersion();

        indexers.extendAndSet((columnIndex) / 2, indexer);
        populateDenseIndexerList();

        TableColumnMetadata columnMetadata = metadata.getColumnQuick(columnIndex);
        columnMetadata.setIndexed(true);
        columnMetadata.setIndexValueBlockCapacity(indexValueBlockSize);

        LOG.info().$("ADDED index to '").utf8(columnName).$('[').$(ColumnType.nameOf(existingType)).$("]' to ").$(path).$();
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

        bumpStructureVersion();
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

            if (mergeRowCount > 0) {
                mergeOutOfOrderRecords();
            }

            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(commitMode);
            }

            updateIndexes();

            txMem.putLong(TX_OFFSET_TXN, ++txn);
            Unsafe.getUnsafe().storeFence();

            txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, transientRowCount);

            if (txPartitionCount > 1) {
                commitPendingPartitions();
                txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, fixedRowCount);
                txPendingPartitionSizes.jumpTo(0);
                txPartitionCount = 1;
            }

            txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, minTimestamp);
            txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, maxTimestamp);

            // store symbol counts
            for (int i = 0, n = denseSymbolMapWriters.size(); i < n; i++) {
                txMem.putInt(getSymbolWriterIndexOffset(i), denseSymbolMapWriters.getQuick(i).getSymbolCount());
            }

            Unsafe.getUnsafe().storeFence();
            txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
            if (commitMode != CommitMode.NOSYNC) {
                txMem.sync(0, commitMode == CommitMode.ASYNC);
            }
            txPrevTransientRowCount = transientRowCount;
        }
    }

    public int getColumnIndex(CharSequence name) {
        int index = metadata.getColumnIndexQuiet(name);
        if (index > -1) {
            return index;
        }
        throw CairoException.instance(0).put("Invalid column name: ").put(name);
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
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
        return structureVersion;
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != txPrevTransientRowCount;
    }

    public boolean isOpen() {
        return tempMem8b != 0;
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
            maxTimestamp = prevMaxTimestamp = Long.MIN_VALUE;
            minTimestamp = prevMinTimestamp = Long.MAX_VALUE;
            timestampSetter = value -> {
            };
        }

        try {
            // open _meta file
            openMetaFile();

            // remove _todo
            removeTodoFile();

            // remove column files has to be done after _todo is removed
            removeColumnFiles(name, type, REMOVE_OR_LOG);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        bumpStructureVersion();

        metadata.removeColumn(name);
        if (timestamp) {
            metadata.setTimestampIndex(-1);
        } else if (timestampColumnName != null) {
            metadata.setTimestampIndex(metadata.getColumnIndex(timestampColumnName));
        }

        LOG.info().$("REMOVED column '").utf8(name).$("' from ").$(path).$();
    }

    public boolean removePartition(long timestamp) {

        if (partitionBy == PartitionBy.NONE || timestamp < timestampFloorMethod.floor(minTimestamp) || timestamp > maxTimestamp) {
            return false;
        }

        if (timestampFloorMethod.floor(timestamp) == timestampFloorMethod.floor(maxTimestamp)) {
            LOG.error()
                    .$("cannot remove active partition [path=").$(path)
                    .$(", maxTimestamp=").$ts(maxTimestamp)
                    .$(']').$();
            return false;
        }

        if (removedPartitions.contains(timestamp)) {
            LOG.error().$("partition is already marked for delete [path=").$(path).$(']').$();
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

            final long nextMinTimestamp;
            if (timestampFloorMethod.floor(timestamp) == timestampFloorMethod.floor(minTimestamp)) {
                nextMinTimestamp = getNextMinTimestamp(timestampFloorMethod, timestampAddMethod);
            } else {
                nextMinTimestamp = minTimestamp;
            }

            setStateForTimestamp(path, timestamp, false);

            if (ff.exists(path)) {

                // todo: when this fails - rescan partitions to calculate fixedRowCount
                //     also write a _todo_ file, which will indicate which partition we wanted to delete
                //     reconcile partitions we can read sizes of with partition table
                //     add partitions we cannot read sizes of to partition table
                final long partitionSize = readPartitionSize(ff, path, tempMem8b);

                int symbolWriterCount = denseSymbolMapWriters.size();
                int partitionTableSize = txMem.getInt(getPartitionTableSizeOffset(symbolWriterCount));

                long txn = txMem.getLong(TX_OFFSET_TXN) + 1;
                txMem.putLong(TX_OFFSET_TXN, txn);
                Unsafe.getUnsafe().storeFence();

                final long partitionVersion = txMem.getLong(TX_OFFSET_PARTITION_TABLE_VERSION) + 1;
                txMem.jumpTo(getPartitionTableIndexOffset(symbolWriterCount, partitionTableSize));
                txMem.putLong(timestamp);

                txMem.putLong(TX_OFFSET_PARTITION_TABLE_VERSION, partitionVersion);
                txMem.putInt(getPartitionTableSizeOffset(symbolWriterCount), partitionTableSize + 1);

                if (nextMinTimestamp != minTimestamp) {
                    txMem.putLong(TX_OFFSET_MIN_TIMESTAMP, nextMinTimestamp);
                    minTimestamp = nextMinTimestamp;
                }

                // decrement row count
                txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT) - partitionSize);

                Unsafe.getUnsafe().storeFence();
                // txn check
                txMem.putLong(TX_OFFSET_TXN_CHECK, txn);

                if (!ff.rmdir(path.chopZ().put(Files.SEPARATOR).$())) {
                    LOG.info().$("partition directory delete is postponed [path=").$(path).$(']').$();
                }

                removedPartitions.add(timestamp);
                fixedRowCount -= partitionSize;

                LOG.info().$("partition marked for delete [path=").$(path).$(']').$();
                return true;
            } else {
                LOG.error().$("cannot remove already missing partition [path=").$(path).$(']').$();
                return false;
            }
        } finally {
            path.trimTo(rootLen);
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
            openMetaFile();

            // remove _todo
            removeTodoFile();

            // rename column files has to be done after _todo is removed
            renameColumnFiles(currentName, newName, type);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        bumpStructureVersion();

        metadata.renameColumn(currentName, newName);

        LOG.info().$("RENAMED column '").utf8(currentName).$("' to '").utf8(newName).$("' from ").$(path).$();
    }

    public void rollback() {
        checkDistressed();
        if (inTransaction()) {
            LOG.info().$("tx rollback [name=").$(name).$(']').$();
            freeColumns(false);
            txPendingPartitionSizes.jumpTo(0);
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
        return fixedRowCount + transientRowCount;
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

        if (size() == 0) {
            return;
        }

        writeTodo(TODO_TRUNCATE);
        for (int i = 0; i < columnCount; i++) {
            getPrimaryColumn(i).truncate();
            AppendMemory mem = getSecondaryColumn(i);
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

        prevMaxTimestamp = Long.MIN_VALUE;
        maxTimestamp = Long.MIN_VALUE;
        prevMinTimestamp = Long.MAX_VALUE;
        minTimestamp = Long.MAX_VALUE;
        txPrevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        txn++;
        txPartitionCount = 1;

        resetTxn(txMem, metadata.getSymbolMapCount(), txn, ++dataVersion);
        try {
            removeTodoFile();
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
            openMetaFile();
        } catch (CairoException err) {
            throwDistressException(err);
        }

        bumpStructureVersion();
        metadata.setTableVersion();
    }

    /**
     * Eagerly sets up writer instance. Otherwise writer will initialize lazily. Invoking this method could improve
     * performance of some applications. UDP receivers use this in order to avoid initial receive buffer contention.
     */
    public void warmUp() {
        Row r = newRow(maxTimestamp);
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

    private static int getPrimaryColumnIndex(int index) {
        return index * 2;
    }

    private static int getSecondaryColumnIndex(int index) {
        return getPrimaryColumnIndex(index) + 1;
    }

    private static void setColumnSize(FilesFacade ff, AppendMemory mem1, AppendMemory mem2, int type, long actualPosition, long buf) {
        long offset;
        long len;
        if (actualPosition > 0) {
            // subtract column top
            switch (type) {
                case ColumnType.BINARY:
                    assert mem2 != null;
                    readOffsetBytes(ff, mem2, actualPosition, buf);
                    offset = Unsafe.getUnsafe().getLong(buf);
                    readBytes(ff, mem1, buf, Long.BYTES, offset, "Cannot read length, fd=");
                    len = Unsafe.getUnsafe().getLong(buf);
                    mem1.setSize(len == -1 ? offset + Long.BYTES : offset + len + Long.BYTES);
                    mem2.setSize(actualPosition * Long.BYTES);
                    break;
                case ColumnType.STRING:
                    assert mem2 != null;
                    readOffsetBytes(ff, mem2, actualPosition, buf);
                    offset = Unsafe.getUnsafe().getLong(buf);
                    readBytes(ff, mem1, buf, Integer.BYTES, offset, "Cannot read length, fd=");
                    len = Unsafe.getUnsafe().getInt(buf);
                    mem1.setSize(len == -1 ? offset + Integer.BYTES : offset + len * Character.BYTES + Integer.BYTES);
                    mem2.setSize(actualPosition * Long.BYTES);
                    break;
                default:
                    mem1.setSize(actualPosition << ColumnType.pow2SizeOf(type));
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
    private static int getColumnIndexQuiet(ReadOnlyMemory metaMem, CharSequence name, int columnCount) {
        long nameOffset = getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence col = metaMem.getStr(nameOffset);
            if (Chars.equalsIgnoreCase(col, name)) {
                return i;
            }
            nameOffset += ContiguousVirtualMemory.getStorageLength(col);
        }
        return -1;
    }

    private static void readOffsetBytes(FilesFacade ff, AppendMemory mem, long position, long buf) {
        readBytes(ff, mem, buf, 8, (position - 1) * 8, "could not read offset, fd=");
    }

    private static void readBytes(FilesFacade ff, AppendMemory mem, long buf, int byteCount, long offset, CharSequence errorMsg) {
        if (ff.read(mem.getFd(), buf, byteCount, offset) != byteCount) {
            throw CairoException.instance(ff.errno()).put(errorMsg).put(mem.getFd()).put(", offset=").put(offset);
        }
    }

    static long searchIndex(long indexAddress, long value, long low, long high, int scanDirection) {
        long mid;
        while (low < high) {
            mid = (low + high) / 2;
            final long midVal = getTimestampIndexValue(indexAddress, mid);
            if (midVal < value)
                if (low < mid) {
                    low = mid;
                } else {
                    return getTimestampIndexValue(indexAddress, high) > value ? low : high;
                }
            else if (midVal > value)
                high = mid;
            else {
                mid += scanDirection;
                while (mid > 0 && mid <= high && midVal == getTimestampIndexValue(indexAddress, mid)) {
                    mid += scanDirection;
                }
                return mid - scanDirection;
            }
        }
        return -high;
    }

    private static int msGetOffsetDataFd(int columnIndex) {
        return columnIndex * 16;
    }

    private static int msGetOffsetTargetFd(int columnIndex) {
        return columnIndex * 16 + 8;
    }

    private static long mapReadOnlyOrFail(FilesFacade ff, Path path, long fd, long size) {
        long dataAddr = ff.mmap(fd, size, 0, Files.MAP_RO);
        if (dataAddr == -1) {
            throw CairoException.instance(ff.errno()).put("could not mmap file read-only [file=").put(path).put(", offset=0, size=").put(size + Integer.BYTES);
        }
        return dataAddr;
    }

    private static long mapReadWriteOrFail(FilesFacade ff, @Nullable Path path, long fd, long size) {
        return mapReadWriteOrFail(ff, path, fd, 0, size);
    }

    private static long mapReadWriteOrFail(FilesFacade ff, @Nullable Path path, long fd, long offset, long size) {
        long addr = ff.mmap(fd, size, offset, Files.MAP_RW);
        if (addr != -1) {
            return addr;
        }
        throw CairoException.instance(ff.errno()).put("could not mmap [file=").put(path).put(", fd=").put(fd).put(", size=").put(size).put(']');
    }

    private static void truncateToSizeOrFail(FilesFacade ff, @Nullable Path path, long fd, long size) {
        if (!ff.truncate(fd, size)) {
            throw CairoException.instance(ff.errno()).put("could resize [file=").put(path).put(", size=").put(size).put(", fd=").put(fd).put(']');
        }
    }

    private static long openReadWriteOrFail(FilesFacade ff, Path path) {
        final long fd = ff.openRW(path);
        if (fd != -1) {
            return fd;
        }
        throw CairoException.instance(ff.errno()).put("could not open for append [file=").put(path).put(']');
    }

    private static long getTimestampIndexRow(long timestampIndex, long indexRow) {
        return Unsafe.getUnsafe().getLong(timestampIndex + indexRow * 16 + Long.BYTES);
    }

    private static long getTimestampIndexValue(long timestampIndex, long indexRow) {
        return Unsafe.getUnsafe().getLong(timestampIndex + indexRow * 16);
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
                nameOffset += ContiguousVirtualMemory.getStorageLength(columnName);
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

    private void bumpStructureVersion() {
        txMem.putLong(TX_OFFSET_TXN, ++txn);
        Unsafe.getUnsafe().storeFence();

        txMem.putLong(TX_OFFSET_STRUCT_VERSION, ++structureVersion);

        final int count = denseSymbolMapWriters.size();
        final int oldCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        txMem.putInt(TX_OFFSET_MAP_WRITER_COUNT, count);
        for (int i = 0; i < count; i++) {
            txMem.putInt(getSymbolWriterIndexOffset(i), denseSymbolMapWriters.getQuick(i).getSymbolCount());
        }

        // when symbol column is removed partition table has to be moved up
        // to do that we just write partition table behind symbol writer table

        if (oldCount != count) {
            int n = removedPartitions.size();
            txMem.putInt(getPartitionTableSizeOffset(count), n);
            for (int i = 0; i < n; i++) {
                txMem.putLong(getPartitionTableIndexOffset(count, i), removedPartitions.get(i));
            }
        }

        Unsafe.getUnsafe().storeFence();
        txMem.putLong(TX_OFFSET_TXN_CHECK, txn);
    }

    private void cancelRow() {

        if ((masterRef & 1) == 0) {
            return;
        }

        if (transientRowCount == 0) {
            if (partitionBy != PartitionBy.NONE) {
                // we have to undo creation of partition
                freeColumns(false);
                if (removeDirOnCancelRow) {
                    try {
                        setStateForTimestamp(path, maxTimestamp, false);
                        if (!ff.rmdir(path.$())) {
                            throw CairoException.instance(ff.errno()).put("Cannot remove directory: ").put(path);
                        }
                        removeDirOnCancelRow = false;
                    } finally {
                        path.trimTo(rootLen);
                    }
                }

                // open old partition
                if (prevMaxTimestamp > Long.MIN_VALUE) {
                    try {
                        txPendingPartitionSizes.jumpTo((txPartitionCount - 2) * 16);
                        openPartition(prevMaxTimestamp);
                        setAppendPosition(txPrevTransientRowCount);
                        txPartitionCount--;
                    } catch (CairoException e) {
                        freeColumns(false);
                        throw e;
                    }
                } else {
                    rowFunction = openPartitionFunction;
                }

                // undo counts
                transientRowCount = txPrevTransientRowCount;
                fixedRowCount -= txPrevTransientRowCount;
                maxTimestamp = prevMaxTimestamp;
                minTimestamp = prevMinTimestamp;
                removeDirOnCancelRow = true;
            } else {
                maxTimestamp = prevMaxTimestamp;
                minTimestamp = prevMinTimestamp;
                // we only have one partition, jump to start on every column
                for (int i = 0; i < columnCount; i++) {
                    getPrimaryColumn(i).setSize(0);
                    AppendMemory mem = getSecondaryColumn(i);
                    if (mem != null) {
                        mem.setSize(0);
                    }
                }
            }
        } else {
            maxTimestamp = prevMaxTimestamp;
            minTimestamp = prevMinTimestamp;
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
                setAppendPosition(transientRowCount);
            }
        }
        refs.fill(0, columnCount, --masterRef);
    }

    private void cancelRowAndBump() {
        cancelRow();
        masterRef++;
    }

    private long ceilMaxTimestamp() {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return Timestamps.ceilDD(this.maxTimestamp);
            case PartitionBy.MONTH:
                return Timestamps.ceilMM(this.maxTimestamp);
            case PartitionBy.YEAR:
                return Timestamps.ceilYYYY(this.maxTimestamp);
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

    private void commitPendingPartitions() {
        long offset = 0;
        for (int i = 0; i < txPartitionCount - 1; i++) {
            try {
                long partitionTimestamp = txPendingPartitionSizes.getLong(offset + 8);
                setStateForTimestamp(path, partitionTimestamp, false);
                long fd = openReadWriteOrFail(ff, path.concat(ARCHIVE_FILE_NAME).$());
                try {
                    int len = 8;
                    long o = offset;
                    while (len > 0) {
                        if (ff.write(fd, txPendingPartitionSizes.addressOf(o), len, 0) == len) {
                            o += len;
                            len = 0;
                        } else {
                            throw CairoException.instance(ff.errno()).put("Commit failed, file=").put(path);
                        }
                    }
                } finally {
                    ff.close(fd);
                }
                offset += 16;
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void configureAppendPosition() {
        this.txn = txMem.getLong(TX_OFFSET_TXN);
        this.transientRowCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
        this.txPrevTransientRowCount = this.transientRowCount;
        this.fixedRowCount = txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
        this.minTimestamp = txMem.getLong(TX_OFFSET_MIN_TIMESTAMP);
        this.maxTimestamp = txMem.getLong(TX_OFFSET_MAX_TIMESTAMP);
        this.dataVersion = txMem.getLong(TX_OFFSET_DATA_VERSION);
        this.structureVersion = txMem.getLong(TX_OFFSET_STRUCT_VERSION);
        this.prevMaxTimestamp = this.maxTimestamp;
        this.prevMinTimestamp = this.minTimestamp;
        if (this.maxTimestamp > Long.MIN_VALUE || partitionBy == PartitionBy.NONE) {
            openFirstPartition(this.maxTimestamp);
            if (partitionBy == PartitionBy.NONE) {
                rowFunction = noPartitionFunction;
            } else {
                rowFunction = switchPartitionFunction;
            }
        } else {
            rowFunction = openPartitionFunction;
        }
    }

    private void configureColumn(int type, boolean indexFlag) {
        final AppendMemory primary = new AppendMemory();
        final AppendMemory secondary;
        final ContiguousVirtualMemory mergePrimary = new ContiguousVirtualMemory(16 * Numbers.SIZE_1MB, Integer.MAX_VALUE);
        final ContiguousVirtualMemory mergeSecondary;
        switch (type) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                secondary = new AppendMemory();
                mergeSecondary = new ContiguousVirtualMemory(16 * Numbers.SIZE_1MB, Integer.MAX_VALUE);
                break;
            default:
                secondary = null;
                mergeSecondary = null;
                break;
        }
        columns.add(primary);
        columns.add(secondary);
        mergeColumns.add(mergePrimary);
        mergeColumns.add(mergeSecondary);
        configureNuller(type, primary, secondary);
        if (indexFlag) {
            indexers.extendAndSet((columns.size() - 1) / 2, new SymbolColumnIndexer());
            populateDenseIndexerList();
        }
        refs.add(0);
    }

    private void configureColumnMemory() {
        int expectedMapWriters = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        long nextSymbolCountOffset = getSymbolWriterIndexOffset(0);
        this.symbolMapWriters.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            configureColumn(type, metadata.isColumnIndexed(i));

            if (type == ColumnType.SYMBOL) {
                assert nextSymbolCountOffset < getSymbolWriterIndexOffset(expectedMapWriters);
                // keep symbol map writers list sparse for ease of access
                SymbolMapWriter symbolMapWriter = new SymbolMapWriter(configuration, path.trimTo(rootLen), metadata.getColumnName(i), txMem.getInt(nextSymbolCountOffset));
                symbolMapWriters.extendAndSet(i, symbolMapWriter);
                denseSymbolMapWriters.add(symbolMapWriter);
                nextSymbolCountOffset += 4;
            }

            if (metadata.isColumnIndexed(i)) {
                indexers.extendAndSet(i, new SymbolColumnIndexer());
            }
        }
        final int timestampIndex = metadata.getTimestampIndex();
        if (timestampIndex != -1) {
            // todo: when column is removed we need to update this again to reflect shifted columns
            timestampMergeMem = mergeColumns.getQuick(getPrimaryColumnIndex(timestampIndex));
        }
        populateDenseIndexerList();
    }

    private void configureNuller(int type, AppendMemory mem1, AppendMemory mem2) {
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

    private LongConsumer configureTimestampSetter() {
        int index = metadata.getTimestampIndex();
        if (index == -1) {
            return value -> {
            };
        } else {
            nullers.setQuick(index, NOOP);
            return getPrimaryColumn(index)::putLong;
        }
    }

    private void copyFixedSizeCol(
            long src,
            long[] mergeStruct,
            int columnIndex,
            long srcLo,
            long srcHi,
            final int shl
    ) {
        final long len = (srcHi - srcLo + 1) << shl;
        final long dst = MergeStruct.getDestFixedAddress(mergeStruct, columnIndex);
        final long offset = MergeStruct.getDestFixedAppendOffset(mergeStruct, columnIndex);
        Unsafe.getUnsafe().copyMemory(src + (srcLo << shl), dst + offset, len);
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, offset + len);
    }

    private void copyFromTimestampIndex(
            long src,
            long[] mergeStruct,
            int columnIndex,
            long srcLo,
            long srcHi
    ) {
        final int shl = 4;
        final long lo = srcLo << shl;
        final long hi = (srcHi + 1) << shl;
        final long start = src + lo;
        final long destOffset = MergeStruct.getDestFixedAppendOffset(mergeStruct, columnIndex);
        final long dest = MergeStruct.getDestFixedAddress(mergeStruct, columnIndex) + destOffset;
        final long len = hi - lo;
        for (long l = 0; l < len; l += 16) {
            Unsafe.getUnsafe().putLong(dest + l / 2, Unsafe.getUnsafe().getLong(start + l));
        }
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, destOffset + len / 2);
    }

    private void copyIndex(long[] mergeStruct, long dataOOMergeIndex, long dataOOMergeIndexLen, int columnIndex) {
        final long offset = MergeStruct.getDestFixedAppendOffset(mergeStruct, columnIndex);
        final long dst = MergeStruct.getDestFixedAddress(mergeStruct, columnIndex) + offset;
        for (long l = 0; l < dataOOMergeIndexLen; l++) {
            Unsafe.getUnsafe().putLong(
                    dst + l * Long.BYTES,
                    getTimestampIndexValue(dataOOMergeIndex, l)
            );
        }
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, offset + dataOOMergeIndexLen * Long.BYTES);
    }

    private int copyMetadataAndSetIndexed(int columnIndex, int indexValueBlockSize) {
        try {
            int index = openMetaSwapFile(ff, ddlMem, path, rootLen, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            ddlMem.putInt(columnCount);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            ddlMem.putInt(ColumnType.VERSION);
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
                nameOffset += ContiguousVirtualMemory.getStorageLength(columnName);
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
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += ContiguousVirtualMemory.getStorageLength(columnName);
            }
            return index;
        } finally {
            ddlMem.close();
        }
    }

    private void copyOOFixed(
            ContiguousVirtualMemory mem,
            long[] mergeStruct,
            int columnIndex,
            long srcLo,
            long srcHi,
            final int shl
    ) {
        copyFixedSizeCol(mem.addressOf(0), mergeStruct, columnIndex, srcLo, srcHi, shl);
    }

    private void copyOutOfOrderData(long indexLo, long indexHi, long[] mergeStruct, int timestampIndex) {
        for (int i = 0; i < columnCount; i++) {
            ContiguousVirtualMemory mem = mergeColumns.getQuick(getPrimaryColumnIndex(i));
            ContiguousVirtualMemory mem2;
            final int columnType = metadata.getColumnType(i);

            switch (columnType) {
                case ColumnType.STRING:
                case ColumnType.BINARY:
                    // we can find out the edge of string column in one of two ways
                    // 1. if indexHi is at the limit of the page - we need to copy the whole page of strings
                    // 2  if there are more items behind indexHi we can get offset of indexHi+1
                    mem2 = mergeColumns.getQuick(getSecondaryColumnIndex(i));
                    copyVarSizeCol(
                            mem2.addressOf(0),
                            mem2.getAppendOffset(),
                            mem.addressOf(0),
                            mem.getAppendOffset(),
                            mergeStruct,
                            i,
                            indexLo,
                            indexHi
                    );
                    break;
                case ColumnType.BOOLEAN:
                case ColumnType.BYTE:
                    copyOOFixed(mem, mergeStruct, i, indexLo, indexHi, 0);
                    break;
                case ColumnType.CHAR:
                case ColumnType.SHORT:
                    copyOOFixed(mem, mergeStruct, i, indexLo, indexHi, 1);
                    break;
                case ColumnType.INT:
                case ColumnType.FLOAT:
                case ColumnType.SYMBOL:
                    copyOOFixed(mem, mergeStruct, i, indexLo, indexHi, 2);
                    break;
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.DOUBLE:
                    copyOOFixed(mem, mergeStruct, i, indexLo, indexHi, 3);
                    break;
                case ColumnType.TIMESTAMP:
                    if (i != timestampIndex) {
                        copyOOFixed(mem, mergeStruct, i, indexLo, indexHi, 3);
                    } else {
                        copyFromTimestampIndex(mem.addressOf(0), mergeStruct, i, indexLo, indexHi);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private void copyPartitionData(long indexLo, long indexHi, long[] mergeStruct) {
        for (int i = 0; i < columnCount; i++) {
            final int columnType = metadata.getColumnType(i);
            switch (columnType) {
                case ColumnType.STRING:
                case ColumnType.BINARY:
                    copyVarSizeCol(
                            MergeStruct.getSrcFixedAddress(mergeStruct, i),
                            MergeStruct.getSrcFixedAddressSize(mergeStruct, i),
                            MergeStruct.getSrcVarAddress(mergeStruct, i),
                            MergeStruct.getSrcVarAddressSize(mergeStruct, i),
                            mergeStruct,
                            i,
                            indexLo,
                            indexHi
                    );
                    break;
                default:
                    copyFixedSizeCol(
                            MergeStruct.getSrcFixedAddress(mergeStruct, i),
                            mergeStruct,
                            i,
                            indexLo,
                            indexHi,
                            ColumnType.pow2SizeOf(columnType)
                    );
                    break;
            }
        }
    }

    private void copyTempPartitionBack(long[] mergeStruct) {
        for (int i = 0; i < columnCount; i++) {
            copyTempPartitionColumnBack(mergeStruct, msGetOffsetDataFd(i));
            copyTempPartitionColumnBack(mergeStruct, msGetOffsetTargetFd(i));
        }
    }

    private void copyTempPartitionColumnBack(long[] mergeStruct, int offset) {
        long fd = Math.abs(mergeStruct[offset]);
        if (fd != 0) {
            long destOldMem = mergeStruct[offset + 1];
            long destOldSize = mergeStruct[offset + 2];

            long srcMem = mergeStruct[offset + 4];
            long srcLen = mergeStruct[offset + 5];


            final long dest = ff.mmap(fd, srcLen, 0, Files.MAP_RW);
            ff.munmap(destOldMem, destOldSize);
            if (dest == -1) {
                throw CairoException.instance(ff.errno())
                        .put("could not remap rw [fd=").put(fd)
                        .put(", oldSize=").put(destOldSize)
                        .put(", newSize=").put(srcLen);
            }
            mergeStruct[offset + 1] = dest;
            mergeStruct[offset + 2] = srcLen;
            Unsafe.getUnsafe().copyMemory(srcMem, dest, srcLen);
        }
    }

    private void copyVarSizeCol(
            long srcFixed,
            long srcFixedSize,
            long srcVar,
            long srcVarSize,
            long[] mergeStruct, int columnIndex, long indexLo,
            long indexHi
    ) {
        final long lo = Unsafe.getUnsafe().getLong(srcFixed + indexLo * Long.BYTES);
        final long hi;
        if (indexHi + 1 == srcFixedSize / Long.BYTES) {
            hi = srcVarSize;
        } else {
            hi = Unsafe.getUnsafe().getLong(srcFixed + (indexHi + 1) * Long.BYTES);
        }
        // copy this before it changes
        final long offset = MergeStruct.getDestVarAppendOffset(mergeStruct, columnIndex);
        final long dest = MergeStruct.getDestVarAddress(mergeStruct, columnIndex) + offset;
        final long len = hi - lo;
        assert offset + len <= MergeStruct.getDestVarAddressSize(mergeStruct, columnIndex);
        Unsafe.getUnsafe().copyMemory(srcVar + lo, dest, len);
        MergeStruct.setDestVarAppendOffset(mergeStruct, columnIndex, offset + len);
        if (lo == offset) {
            copyFixedSizeCol(srcFixed, mergeStruct, columnIndex, indexLo, indexHi, 3);
        } else {
            shiftCopyFixedSizeColumnData(lo - offset, srcFixed, mergeStruct, columnIndex, indexLo, indexHi);
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
                LOG.error().$("could not create index [name=").utf8(path).$(']').$();
                if (!ff.remove(path)) {
                    LOG.error().$("could not remove '").utf8(path).$("'. Please remove MANUALLY.").$();
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
        SymbolMapWriter w = new SymbolMapWriter(configuration, path, name, 0);
        denseSymbolMapWriters.add(w);
        symbolMapWriters.extendAndSet(columnCount, w);
    }

    private long[] createTempPartition(
            Path path,
            long indexLo,
            long indexHi,
            long indexMax,
            long dataIndexMax,
            int destIndex,
            boolean reuseFds,
            int timestampIndex,
            long timestampFd
    ) {
        long[] mergeStruct = new long[columnCount * MergeStruct.MERGE_STRUCT_ENTRY_SIZE];

        int plen = path.length();
        for (int i = 0; i < columnCount; i++) {
            ContiguousVirtualMemory mem = mergeColumns.getQuick(getPrimaryColumnIndex(i));
            ContiguousVirtualMemory mem2;
            final int columnType = metadata.getColumnType(i);

            long lo;
            long hi;
            int shl;
            long dataSize;
            try {
                switch (columnType) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        shl = ColumnType.pow2SizeOf(ColumnType.LONG);

                        if (dataIndexMax > 0) {
                            // index files are opened as normal
                            iFile(path, metadata.getColumnName(i));

                            long indexFd = reuseFds ? -columns.getQuick(getSecondaryColumnIndex(i)).getFd() : openReadWriteOrFail(ff, path);
                            LOG.info().$("open [fd=").$(indexFd).$(", path=`").utf8(path).$("`]").$();
                            MergeStruct.mergeStructSetSrcFixedFd(mergeStruct, i, indexFd);

                            long indexSize = dataIndexMax << shl;
                            long indexAddr = mapReadWriteOrFail(ff, path, Math.abs(indexFd), indexSize);
                            if (indexFd < 0) {
                                columns.getQuick(getSecondaryColumnIndex(i)).releaseCurrentPage();
                            }

                            MergeStruct.setSrcFixedAddress(mergeStruct, i, indexAddr);
                            MergeStruct.setSrcFixedAddressSize(mergeStruct, i, indexSize);

                            // open data file now

                            path.trimTo(plen);
                            dFile(path, metadata.getColumnName(i));
                            final long dataFd;
                            if (reuseFds) {
                                final AppendMemory m = columns.getQuick(getPrimaryColumnIndex(i));
                                dataFd = -m.getFd();
                                dataSize = m.getAppendOffset();
                            } else {
                                dataFd = openReadWriteOrFail(ff, path);
                                dataSize = ff.length(dataFd);
                                if (dataSize == -1) {
                                    throw CairoException.instance(ff.errno()).put("Could not fstat [fd=").put(dataFd).put(']');
                                }
                            }
                            MergeStruct.setSrcVarFd(mergeStruct, i, dataFd);
                            long dataAddr = mapReadOnlyOrFail(ff, path, Math.abs(dataFd), dataSize);
                            if (dataFd < 0) {
                                columns.getQuick(getPrimaryColumnIndex(i)).releaseCurrentPage();
                            }
                            MergeStruct.setSrcVarAddress(mergeStruct, i, dataAddr);
                            MergeStruct.setSrcVarAddressSize(mergeStruct, i, dataSize);
                            LOG.info().$("open [fd=").$(dataFd).$(", path=`").utf8(path).$("`, addr=").$(dataAddr).$(", size=").$(dataSize).$(']').$();
                        } else {
                            dataSize = 0;
                        }

                        path.trimTo(plen);
                        if (destIndex > 0) {
                            path.put('.').put(destIndex);
                        }
                        iFile(path, metadata.getColumnName(i));

                        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
                        }

                        final long indexMergeFd = openReadWriteOrFail(ff, path);

                        MergeStruct.setDestFixedFd(mergeStruct, i, indexMergeFd);

                        long indexMergeSize = ((indexHi - indexLo + 1) + dataIndexMax) << shl;

                        truncateToSizeOrFail(ff, path, indexMergeFd, indexMergeSize);
                        long indexMergeAddr = mapReadWriteOrFail(ff, path, indexMergeFd, indexMergeSize);

                        MergeStruct.setDestFixedAddress(mergeStruct, i, indexMergeAddr);
                        MergeStruct.setDestFixedAddressSize(mergeStruct, i, indexMergeSize);
                        MergeStruct.setDestFixedAppendOffset(mergeStruct, i, 0L);
                        LOG.info().$("open [fd=").$(indexMergeFd).$(", path=`").utf8(path).$("`, addr=").$(indexMergeAddr).$(", size=").$(indexMergeSize).$(']').$();

                        path.trimTo(plen);
                        if (destIndex > 0) {
                            path.put('.').put(destIndex);
                        }
                        dFile(path, metadata.getColumnName(i));

                        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
                        }

                        final long dataMergeFd = openReadWriteOrFail(ff, path);

                        MergeStruct.setDestVarFd(mergeStruct, i, dataMergeFd);

                        mem2 = mergeColumns.getQuick(getSecondaryColumnIndex(i));
                        lo = mem2.getLong(indexLo * Long.BYTES);
                        if (indexHi == indexMax - 1) {
                            hi = mem.getAppendOffset();
                        } else {
                            hi = mem2.getLong((indexHi + 1) * Long.BYTES);
                        }

                        dataSize += (hi - lo);

                        truncateToSizeOrFail(ff, path, dataMergeFd, dataSize);
                        final long dataMergeAddr = mapReadWriteOrFail(ff, path, dataMergeFd, dataSize);
                        MergeStruct.getDestVarAddress(mergeStruct, i, dataMergeAddr);
                        MergeStruct.setDestVarAddressSize(mergeStruct, i, dataSize);
                        MergeStruct.setDestVarAppendOffset(mergeStruct, i, 0L);
                        LOG.info().$("open [fd=").$(dataMergeFd).$(", path=`").utf8(path).$("`, addr=").$(dataMergeAddr).$(", size=").$(dataSize).$(']').$();
                        break;

                    default:
                        shl = ColumnType.pow2SizeOf(columnType);

                        if (dataIndexMax > 0) {
                            dFile(path, metadata.getColumnName(i));
                            dataSize = dataIndexMax << shl;

                            long dataFd;
                            if (timestampIndex == i && timestampFd != 0) {
                                // ensure timestamp fd is always negative, we will close it externally
                                dataFd = timestampFd > 0 ? -timestampFd : timestampFd;
                            } else {
                                dataFd = reuseFds ? -columns.getQuick(getPrimaryColumnIndex(i)).getFd() : openReadWriteOrFail(ff, path);
                            }
                            LOG.info().$("open [fd=").$(dataFd).$(", path=`").utf8(path).$("`]").$();
                            MergeStruct.mergeStructSetSrcFixedFd(mergeStruct, i, dataFd);
                            MergeStruct.setSrcFixedAddress(mergeStruct, i, mapReadWriteOrFail(ff, path, Math.abs(dataFd), dataSize));
                            if (dataFd < 0) {
                                columns.getQuick(getPrimaryColumnIndex(i)).releaseCurrentPage();
                            }
                            MergeStruct.setSrcFixedAddressSize(mergeStruct, i, dataSize);
                        }

                        path.trimTo(plen);
                        if (destIndex > 0) {
                            path.put('.').put(destIndex);
                        }
                        dFile(path, metadata.getColumnName(i));

                        if (ff.mkdirs(path, configuration.getMkDirMode()) != 0) {
                            throw CairoException.instance(ff.errno()).put("could not create directories [file=").put(path).put(']');
                        }

                        final long mergeFd = openReadWriteOrFail(ff, path);
                        LOG.info().$("open [fd=").$(mergeFd).$(", path=`").utf8(path).$("`]").$();
                        MergeStruct.setDestFixedFd(mergeStruct, i, mergeFd);
                        final long mergeSize = ((indexHi - indexLo + 1) + dataIndexMax) << shl;
                        truncateToSizeOrFail(ff, path, mergeFd, mergeSize);
                        MergeStruct.setDestFixedAddress(mergeStruct, i, mapReadWriteOrFail(ff, path, mergeFd, mergeSize));
                        MergeStruct.setDestFixedAddressSize(mergeStruct, i, mergeSize);
                        MergeStruct.setDestFixedAppendOffset(mergeStruct, i, 0L);
                        break;
                }
            } finally {
                path.trimTo(plen);
            }
        }
        return mergeStruct;
    }

    private void doClose(boolean truncate) {
        boolean tx = inTransaction();
        freeColumns(truncate);
        freeSymbolMapWriters();
        freeIndexers();
        try {
            freeTxMem();
        } finally {
            Misc.free(metaMem);
            Misc.free(txPendingPartitionSizes);
            Misc.free(ddlMem);
            Misc.free(other);
            Misc.free(tmpShuffleData);
            Misc.free(tmpShuffleIndex);
            Misc.free(timestampSearchColumn);
            try {
                releaseLock(!truncate | tx | performRecovery | distressed);
            } finally {
                Misc.free(path);
                freeTempMem();
                LOG.info().$("closed '").utf8(name).$('\'').$();
            }
        }
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                AppendMemory m = columns.getQuick(i);
                if (m != null) {
                    m.close(truncate);
                }
            }
        }
        Misc.freeObjListAndKeepObjects(mergeColumns);
    }

    private void freeIndexers() {
        if (indexers != null) {
            for (int i = 0, n = indexers.size(); i < n; i++) {
                Misc.free(indexers.getQuick(i));
            }
            indexers.clear();
            denseIndexers.clear();
        }
    }

    private void freeMergeStruct(long[] mergeStruct) {
        for (int i = 0; i < columnCount; i++) {
            freeMergeStructArtefact(mergeStruct, i * 16, true);
            freeMergeStructArtefact(mergeStruct, i * 16 + 3, false);
            freeMergeStructArtefact(mergeStruct, i * 16 + 8, true);
            freeMergeStructArtefact(mergeStruct, i * 16 + 8 + 3, false);
        }
    }

    private void freeMergeStructArtefact(long[] mergeStruct, int offset, boolean truncate) {
        long fd = mergeStruct[offset];
        long mem = mergeStruct[offset + 1];
        long memSize = mergeStruct[offset + 2];

        if (mem != 0 && memSize != 0) {
            ff.munmap(mem, memSize);
            if (truncate) {
                AppendMemory.bestEffortTruncate(ff, LOG, Math.abs(fd), memSize, Files.PAGE_SIZE);
            }
        }

        if (fd > 0) {
            ff.close(fd);
            LOG.info().$("closed [fd=").$(fd).$(']').$();
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
        if (tempMem8b != 0) {
            Unsafe.free(tempMem8b, 8);
            tempMem8b = 0;
        }
    }

    private void freeTxMem() {
        if (txMem != null) {
            try {
                txMem.jumpTo(getTxEofOffset());
            } finally {
                txMem.close();
            }
        }
    }

    private long getNextMinTimestamp(
            Timestamps.TimestampFloorMethod timestampFloorMethod,
            Timestamps.TimestampAddMethod timestampAddMethod
    ) {
        long nextMinTimestamp = minTimestamp;
        while (nextMinTimestamp < maxTimestamp) {
            long nextTimestamp = timestampFloorMethod.floor(timestampAddMethod.calculate(nextMinTimestamp, 1));
            setStateForTimestamp(path, nextTimestamp, false);
            try {
                dFile(path, metadata.getColumnName(metadata.getTimestampIndex()));
                if (ff.exists(path)) {
                    // read min timestamp value
                    long fd = ff.openRO(path);
                    if (fd == -1) {
                        // oops
                        throw CairoException.instance(Os.errno()).put("could not open [file=").put(path).put(']');
                    }
                    try {
                        long buf = Unsafe.malloc(Long.BYTES);
                        try {
                            long n = ff.read(fd, buf, Long.BYTES, 0);
                            if (n != Long.BYTES) {
                                throw CairoException.instance(Os.errno()).put("could not read timestamp value");
                            }
                            nextMinTimestamp = Unsafe.getUnsafe().getLong(buf);
                        } finally {
                            Unsafe.free(buf, Long.BYTES);
                        }
                    } finally {
                        ff.close(fd);
                    }
                    break;
                }
                nextMinTimestamp = nextTimestamp;
            } finally {
                path.trimTo(rootLen);
            }
        }
        assert nextMinTimestamp > minTimestamp;
        return nextMinTimestamp;
    }

    private AppendMemory getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private AppendMemory getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    SymbolMapWriter getSymbolMapWriter(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex);
    }

    private long getTxEofOffset() {
        if (metadata != null) {
            return getTxMemSize(metadata.getSymbolMapCount(), removedPartitions.size());
        } else {
            return ff.length(txMem.getFd());
        }
    }

    int getTxPartitionCount() {
        return txPartitionCount;
    }

    private long indexHistoricPartitions(SymbolColumnIndexer indexer, CharSequence columnName, int indexValueBlockSize) {
        final long maxTimestamp = timestampFloorMethod.floor(this.maxTimestamp);
        long timestamp = minTimestamp;

        try (indexer; final ReadOnlyMemory roMem = new ReadOnlyMemory()) {

            while (timestamp < maxTimestamp) {

                path.trimTo(rootLen);

                setStateForTimestamp(path, timestamp, true);

                if (ff.exists(path.$())) {

                    final int plen = path.length();

                    TableUtils.dFile(path.trimTo(plen), columnName);

                    if (ff.exists(path)) {

                        path.trimTo(plen);

                        LOG.info().$("indexing [path=").$(path).$(']').$();

                        createIndexFiles(columnName, indexValueBlockSize, plen, true);

                        final long partitionSize = TableUtils.readPartitionSize(ff, path.trimTo(plen), tempMem8b);
                        final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, tempMem8b);

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

        final long columnTop = TableUtils.readColumnTop(ff, path.trimTo(plen), columnName, plen, tempMem8b);

        // set indexer up to continue functioning as normal
        indexer.configureFollowerAndWriter(configuration, path.trimTo(plen), columnName, getPrimaryColumn(columnIndex), columnTop);
        indexer.refreshSourceAndIndex(0, transientRowCount);
    }

    boolean isSymbolMapWriterCached(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex).isCached();
    }

    private void loadRemovedPartitions() {
        int symbolWriterCount = denseSymbolMapWriters.size();
        int partitionTableSize = txMem.getInt(getPartitionTableSizeOffset(symbolWriterCount));
        if (partitionTableSize > 0) {
            for (int i = 0; i < partitionTableSize; i++) {
                removedPartitions.add(txMem.getLong(getPartitionTableIndexOffset(symbolWriterCount, i)));
            }
        }
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

    private void mergeCopyBin(long[] mergeStruct, long dataOOMergeIndex, long dataOOMergeIndexLen, int columnIndex) {
        // destination of variable length data
        long destVarOffset = MergeStruct.getDestVarAppendOffset(mergeStruct, columnIndex);
        final long destVar = MergeStruct.getDestVarAddress(mergeStruct, columnIndex);
        final long destFixOffset = MergeStruct.getDestFixedAppendOffset(mergeStruct, columnIndex);
        final long dstFix = MergeStruct.getDestFixedAddress(mergeStruct, columnIndex) + destFixOffset;

        // fixed length column
        final long srcFix1 = MergeStruct.getSrcFixedAddress(mergeStruct, columnIndex);
        final long srcFix2 = mergeColumns.getQuick(getSecondaryColumnIndex(columnIndex)).addressOf(0);

        long srcVar1 = MergeStruct.getSrcVarAddress(mergeStruct, columnIndex);
        long srcVar2 = mergeColumns.getQuick(getPrimaryColumnIndex(columnIndex)).addressOf(0);

        // reverse order
        // todo: cache?
        long[] srcFix = new long[]{srcFix2, srcFix1};
        long[] srcVar = new long[]{srcVar2, srcVar1};

        for (long l = 0; l < dataOOMergeIndexLen; l++) {
            final long row = getTimestampIndexRow(dataOOMergeIndex, l);
            // high bit in the index in the source array [0,1]
            final int bit = (int) (row >>> 63);
            // row number is "row" with high bit removed
            final long rr = row & ~(1L << 63);
            Unsafe.getUnsafe().putLong(dstFix + l * Long.BYTES, destVarOffset);
            long offset = Unsafe.getUnsafe().getLong(srcFix[bit] + rr * Long.BYTES);
            long addr = srcVar[bit] + offset;
            long len = Unsafe.getUnsafe().getLong(addr);
            if (len > 0) {
                Unsafe.getUnsafe().copyMemory(addr, destVar + destVarOffset, len + Long.BYTES);
                destVarOffset += len + Long.BYTES;
            } else {
                Unsafe.getUnsafe().putLong(destVar + destVarOffset, len);
                destVarOffset += Long.BYTES;
            }
        }
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, destFixOffset + dataOOMergeIndexLen * Long.BYTES);
        MergeStruct.setDestVarAppendOffset(mergeStruct, columnIndex, destVarOffset);
    }

    private void mergeCopyStr(long[] mergeStruct, long dataOOMergeIndex, long dataOOMergeIndexLen, int columnIndex) {
        // destination of variable length data
        long destVarOffset = MergeStruct.getDestVarAppendOffset(mergeStruct, columnIndex);
        final long destVar = MergeStruct.getDestVarAddress(mergeStruct, columnIndex);
        final long destFixOffset = MergeStruct.getDestFixedAppendOffset(mergeStruct, columnIndex);
        final long dstFix = MergeStruct.getDestFixedAddress(mergeStruct, columnIndex) + destFixOffset;

        // fixed length column
        final long srcFix1 = MergeStruct.getSrcFixedAddress(mergeStruct, columnIndex);
        final long srcFix2 = mergeColumns.getQuick(getSecondaryColumnIndex(columnIndex)).addressOf(0);

        long srcVar1 = MergeStruct.getSrcVarAddress(mergeStruct, columnIndex);
        long srcVar2 = mergeColumns.getQuick(getPrimaryColumnIndex(columnIndex)).addressOf(0);

        // reverse order
        // todo: cache?
        long[] srcFix = new long[]{srcFix2, srcFix1};
        long[] srcVar = new long[]{srcVar2, srcVar1};

        for (long l = 0; l < dataOOMergeIndexLen; l++) {
            final long row = getTimestampIndexRow(dataOOMergeIndex, l);
            // high bit in the index in the source array [0,1]
            final int bit = (int) (row >>> 63);
            // row number is "row" with high bit removed
            final long rr = row & ~(1L << 63);
            Unsafe.getUnsafe().putLong(dstFix + l * Long.BYTES, destVarOffset);
            long offset = Unsafe.getUnsafe().getLong(srcFix[bit] + rr * Long.BYTES);
            long addr = srcVar[bit] + offset;
            int len = Unsafe.getUnsafe().getInt(addr);
            if (len > 0) {
                Unsafe.getUnsafe().copyMemory(addr, destVar + destVarOffset, len * Character.BYTES + Integer.BYTES);
                destVarOffset += len * Character.BYTES + Integer.BYTES;
            } else {
                Unsafe.getUnsafe().putInt(destVar + destVarOffset, len);
                destVarOffset += Integer.BYTES;
            }
        }
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, destFixOffset + dataOOMergeIndexLen * Long.BYTES);
        MergeStruct.setDestVarAppendOffset(mergeStruct, columnIndex, destVarOffset);
    }

    private void mergeOOAndShuffleColumns(int timestampIndex, long mergedTimestamps, long mergeDataLo, long mergeDataHi, long mergeOOOLo, long mergeOOOHi, long[] mergeStruct) {
        final long dataOOMergeIndexLen = mergeOOOHi - mergeOOOLo + 1 + mergeDataHi - mergeDataLo + 1;
        // copy timestamp column of the partition into a "index" memory

        final long dataOOMergeIndex = mergeTimestampAndOutOfOrder(
                timestampIndex,
                mergedTimestamps,
                mergeDataLo,
                mergeDataHi,
                mergeOOOLo,
                mergeOOOHi,
                mergeStruct
        );

        try {
            for (int i = 0; i < columnCount; i++) {
                switch (metadata.getColumnType(i)) {
                    case ColumnType.BOOLEAN:
                    case ColumnType.BYTE:
                        mergeShuffle(mergeStruct, dataOOMergeIndex, dataOOMergeIndexLen, i, 0, MERGE_SHUFFLE_8);
                        break;
                    case ColumnType.SHORT:
                    case ColumnType.CHAR:
                        mergeShuffle(mergeStruct, dataOOMergeIndex, dataOOMergeIndexLen, i, 1, MERGE_SHUFFLE_16);
                        break;
                    case ColumnType.STRING:
                        mergeCopyStr(mergeStruct, dataOOMergeIndex, dataOOMergeIndexLen, i);
                        break;
                    case ColumnType.BINARY:
                        mergeCopyBin(mergeStruct, dataOOMergeIndex, dataOOMergeIndexLen, i);
                        break;
                    case ColumnType.INT:
                    case ColumnType.FLOAT:
                    case ColumnType.SYMBOL:
                        mergeShuffle(mergeStruct, dataOOMergeIndex, dataOOMergeIndexLen, i, 2, MERGE_SHUFFLE_32);
                        break;
                    case ColumnType.DOUBLE:
                    case ColumnType.LONG:
                    case ColumnType.DATE:
                    case ColumnType.TIMESTAMP:
                        if (i == timestampIndex) {
                            // copy timestamp values from the merge index
                            copyIndex(mergeStruct, dataOOMergeIndex, dataOOMergeIndexLen, i);
                            break;
                        }
                        mergeShuffle(mergeStruct, dataOOMergeIndex, dataOOMergeIndexLen, i, 3, MERGE_SHUFFLE_64);
                        break;
                }
            }
        } finally {
            Vect.freeMergedIndex(dataOOMergeIndex);
        }
    }

    private void mergeOutOfOrderRecords() {
        final int timestampIndex = metadata.getTimestampIndex();
        try {

            // we may need to re-use file descriptors when this partition is the "current" one
            // we cannot open file again due to sharing violation
            //
            // to determine that 'ooTimestampLo' goes into current partition
            // we need to compare 'partitionTimestampHi', which is appropriately truncated to DAY/MONTH/YEAR
            // to this.maxTimestamp, which isn't truncated yet. So we need to truncate it first
            final long ceilOfMaxTimestamp = ceilMaxTimestamp();
            LOG.info().$("sorting [name=").$(name).$(']').$();
            final long mergedTimestamps = timestampMergeMem.addressOf(0);
            Vect.sortLongIndexAscInPlace(mergedTimestamps, mergeRowCount);

            // reshuffle all variable length columns
            for (int i = 0; i < columnCount; i++) {
                final int type = metadata.getColumnType(i);
                switch (type) {
                    case ColumnType.BINARY:
                    case ColumnType.STRING:
                        shuffleVarLenValues(i, mergedTimestamps, mergeRowCount);
                        break;
                    case ColumnType.FLOAT:
                    case ColumnType.INT:
                    case ColumnType.SYMBOL:
                        shuffleFixedLengthValues(i, mergedTimestamps, mergeRowCount, 2, SHUFFLE_32);
                        break;
                    case ColumnType.LONG:
                    case ColumnType.DOUBLE:
                    case ColumnType.DATE:
                        shuffleFixedLengthValues(i, mergedTimestamps, mergeRowCount, 3, SHUFFLE_64);
                        break;
                    case ColumnType.TIMESTAMP:
                        if (i != timestampIndex) {
                            shuffleFixedLengthValues(i, mergedTimestamps, mergeRowCount, 3, SHUFFLE_64);
                        }
                        break;
                    case ColumnType.SHORT:
                    case ColumnType.CHAR:
                        shuffleFixedLengthValues(i, mergedTimestamps, mergeRowCount, 1, SHUFFLE_16);
                        break;
                    case ColumnType.BOOLEAN:
                    case ColumnType.BYTE:
                        shuffleFixedLengthValues(i, mergedTimestamps, mergeRowCount, 0, SHUFFLE_8);
                        break;
                }
            }

            Vect.flattenIndex(mergedTimestamps, mergeRowCount);

            // we have three frames:
            // partition logical "lo" and "hi" - absolute bounds (partitionLo, partitionHi)
            // partition actual data "lo" and "hi" (dataLo, dataHi)
            // out of order "lo" and "hi" (indexLo, indexHi)

            long indexLo = 0;
            long indexHi;
            long indexMax = mergeRowCount;
            long ooTimestampMin = getTimestampIndexValue(mergedTimestamps, 0);
            long ooTimestampHi = getTimestampIndexValue(mergedTimestamps, indexMax - 1);

            while (true) {
                long ooTimestampLo = getTimestampIndexValue(mergedTimestamps, indexLo);

                path.trimTo(rootLen);
                setStateForTimestamp(path, ooTimestampLo, true);
                int plen = path.length();

                final long partitionTimestampHi = this.partitionHi;

                // find "current" partition boundary in the out of order data
                // once we know the boundary we can move on to calculating another one
                if (ooTimestampHi > partitionTimestampHi) {
                    indexHi = searchIndex(mergedTimestamps, partitionTimestampHi, indexLo, indexMax - 1, BinarySearch.SCAN_DOWN);
                } else {
                    indexHi = indexMax - 1;
                }

                long fd = 0;
                long dataTimestampLo;
                long dataTimestampHi = Long.MIN_VALUE;
                long dataIndexMax = 0;

                // is out of order data hitting the last partition?
                // if so we do not need to re-open files and and write to existing file descriptors


                if (partitionTimestampHi > ceilOfMaxTimestamp || partitionTimestampHi < timestampFloorMethod.floor(minTimestamp)) {

                    // this has to be a brand new partition
                    LOG.info().$("copy oo to [path=").$(path).$(", from=").$(indexLo).$(", to=").$(indexHi).$(']').$();
                    // pure OOO data copy into new partition
                    long[] mergeStruct = createTempPartition(
                            path,
                            indexLo,
                            indexHi,
                            indexMax,
                            0,
                            0,
                            false,
                            -1,
                            0
                    );
                    try {
                        copyOutOfOrderData(indexLo, indexHi, mergeStruct, timestampIndex);
                    } finally {
                        freeMergeStruct(mergeStruct);
                    }
                } else {

                    // out of order is hitting existing partition

                    try {
                        dFile(path, metadata.getColumnName(timestampIndex));
                        if (partitionTimestampHi == ceilOfMaxTimestamp) {
                            dataTimestampHi = this.maxTimestamp;
                            dataIndexMax = transientRowCountBeforeOutOfOrder;
                            fd = -columns.getQuick(getPrimaryColumnIndex(timestampIndex)).getFd();
                        } else {

                            // out of order data is going into archive partition
                            // we need to read "low" and "high" boundaries of the partition. "low" being oldest timestamp
                            // and "high" being newest


                            // also track the fd that we need to eventually close
                            fd = ff.openRW(path);
                            if (fd == -1) {
                                throw CairoException.instance(ff.errno()).put("could not open `").put(path).put('`');
                            }

                            // todo: read the archive
                            dataIndexMax = ff.length(fd) / Long.BYTES;

                            // read bottom of file
                            if (ff.read(fd, tempMem8b, Long.BYTES, (dataIndexMax - 1) * Long.BYTES) != Long.BYTES) {
                                throw CairoException.instance(ff.errno()).put("could not read bottom 8 bytes from `").put(path).put('`');
                            }
                            dataTimestampHi = Unsafe.getUnsafe().getLong(tempMem8b);
                        }

                        // read the top value
                        if (ff.read(Math.abs(fd), tempMem8b, Long.BYTES, 0) != Long.BYTES) {
                            throw CairoException.instance(ff.errno()).put("could not read top 8 bytes from `").put(path).put('`');
                        }

                        dataTimestampLo = Unsafe.getUnsafe().getLong(tempMem8b);

                        // create copy jobs
                        // we will have maximum of 3 stages:
                        // - prefix data
                        // - merge job
                        // - suffix data
                        //
                        // prefix and suffix can be sourced either from OO fully or from Data (written to disk) fully
                        // so for prefix and suffix we will need a flag indicating source of the data
                        // as well as range of rows in that source

                        int prefixType = OO_BLOCK_NONE;
                        long prefixLo = -1;
                        long prefixHi = -1;
                        int mergeType = OO_BLOCK_NONE;
                        long mergeDataLo = -1;
                        long mergeDataHi = -1;
                        long mergeOOOLo = -1;
                        long mergeOOOHi = -1;
                        int suffixType = OO_BLOCK_NONE;
                        long suffixLo = -1;
                        long suffixHi = -1;

                        // this is an overloaded function, page size is derived from the file size
                        timestampSearchColumn.of(ff, Math.abs(fd), path, dataIndexMax * Long.BYTES);

                        try {
                            if (ooTimestampLo < dataTimestampLo) {

                                prefixType = OO_BLOCK_OO;
                                prefixLo = indexLo;
                                if (dataTimestampLo < ooTimestampHi) {

                                    //            +-----+
                                    //            | OOO |
                                    //
                                    //  +------+
                                    //  | data |

                                    mergeDataLo = 0;
                                    prefixHi = searchIndex(mergedTimestamps, dataTimestampLo, indexLo, indexHi, BinarySearch.SCAN_DOWN);
                                    mergeOOOLo = prefixHi + 1;
                                } else {
                                    //            +-----+
                                    //            | OOO |
                                    //            +-----+
                                    //
                                    //  +------+
                                    //  | data |
                                    //
                                    prefixHi = indexHi;
                                }

                                if (ooTimestampHi >= dataTimestampLo) {

                                    //
                                    //  +------+  | OOO |
                                    //  | data |  +-----+
                                    //  |      |

                                    if (ooTimestampHi < dataTimestampHi) {

                                        // |      | |     |
                                        // |      | | OOO |
                                        // | data | +-----+
                                        // |      |
                                        // +------+

                                        mergeOOOHi = indexHi;
                                        mergeDataHi = BinarySearch.find(timestampSearchColumn, ooTimestampHi, 0, dataIndexMax - 1, BinarySearch.SCAN_DOWN);
                                    } else if (ooTimestampHi > dataTimestampHi) {

                                        // |      | |     |
                                        // |      | | OOO |
                                        // | data | |     |
                                        // +------+ |     |
                                        //          +-----+

                                        mergeDataHi = dataIndexMax - 1;

                                        // check if data is regressed to single timestamp
                                        if (dataTimestampLo == dataTimestampHi) {
                                            mergeType = OO_BLOCK_DATA;
                                            suffixLo = Math.min(prefixHi + 1, indexHi);
                                        } else {
                                            mergeType = OO_BLOCK_MERGE;
                                            mergeOOOHi = searchIndex(mergedTimestamps, dataTimestampHi, mergeOOOLo, indexHi, BinarySearch.SCAN_UP);
                                            suffixLo = mergeOOOHi + 1;
                                        }

                                        suffixType = OO_BLOCK_OO;
                                        suffixHi = indexHi;
                                    } else {

                                        // |      | |     |
                                        // |      | | OOO |
                                        // | data | |     |
                                        // +------+ +-----+

                                        mergeOOOHi = indexHi;
                                        mergeDataHi = dataIndexMax - 1;
                                    }
                                } else {

                                    //            +-----+
                                    //            | OOO |
                                    //            +-----+
                                    //
                                    //  +------+
                                    //  | data |

                                    suffixType = OO_BLOCK_DATA;
                                    suffixLo = 0;
                                    suffixHi = dataIndexMax - 1;
                                }
                            } else {
                                if (ooTimestampLo <= dataTimestampLo) {
                                    //            +-----+
                                    //            | OOO |
                                    //            +-----+
                                    //
                                    //  +------+
                                    //  | data |
                                    //

                                    prefixType = OO_BLOCK_OO;
                                    prefixLo = indexLo;
                                    prefixHi = indexHi;

                                    suffixType = OO_BLOCK_DATA;
                                    suffixLo = 0;
                                    suffixHi = dataIndexMax - 1;
                                } else {
                                    //   +------+                +------+  +-----+
                                    //   | data |  +-----+       | data |  |     |
                                    //   |      |  | OOO |  OR   |      |  | OOO |
                                    //   |      |  |     |       |      |  |     |

                                    if (ooTimestampLo <= dataTimestampHi) {

                                        //
                                        // +------+
                                        // |      |
                                        // |      | +-----+
                                        // | data | | OOO |
                                        // +------+

                                        prefixType = OO_BLOCK_DATA;
                                        prefixLo = 0;
                                        prefixHi = BinarySearch.find(timestampSearchColumn, ooTimestampLo, 0, dataIndexMax - 1, BinarySearch.SCAN_UP);
                                        mergeDataLo = prefixHi + 1;
                                        mergeOOOLo = indexLo;

                                        if (ooTimestampHi < dataTimestampHi) {

                                            //
                                            // |      | +-----+
                                            // | data | | OOO |
                                            // |      | +-----+
                                            // +------+

                                            mergeOOOHi = indexHi;
                                            mergeDataHi = BinarySearch.find(timestampSearchColumn, ooTimestampHi, mergeDataLo, dataIndexMax - 1, BinarySearch.SCAN_DOWN);

                                            suffixType = OO_BLOCK_DATA;
                                            suffixLo = mergeDataHi + 1;
                                            suffixHi = dataIndexMax - 1;
                                        } else if (ooTimestampHi > dataTimestampHi) {

                                            //
                                            // |      | +-----+
                                            // | data | | OOO |
                                            // |      | |     |
                                            // +------+ |     |
                                            //          |     |
                                            //          +-----+

                                            mergeOOOHi = searchIndex(mergedTimestamps, dataTimestampHi, indexLo, indexHi, BinarySearch.SCAN_UP);
                                            mergeDataHi = dataIndexMax - 1;

                                            mergeType = OO_BLOCK_MERGE;
                                            suffixType = OO_BLOCK_OO;
                                            suffixLo = mergeOOOHi + 1;
                                            suffixHi = indexHi;
                                        } else {

                                            //
                                            // |      | +-----+
                                            // | data | | OOO |
                                            // |      | |     |
                                            // +------+ +-----+
                                            //

                                            mergeOOOHi = indexHi;
                                            mergeDataHi = dataIndexMax - 1;
                                        }
                                    } else {

                                        // +------+
                                        // | data |
                                        // |      |
                                        // +------+
                                        //
                                        //           +-----+
                                        //           | OOO |
                                        //           |     |
                                        //
                                        suffixType = OO_BLOCK_OO;
                                        suffixLo = indexLo;
                                        suffixHi = indexHi;
                                    }
                                }
                            }
                        } finally {
                            // disconnect memory from fd, so that we don't accidentally close it
                            timestampSearchColumn.detach();
                        }

                        path.trimTo(plen);
                        final long[] mergeStruct;
                        if (prefixType == OO_BLOCK_NONE && mergeType == OO_BLOCK_NONE) {
                            mergeStruct = prepareAppendMemory(
                                    indexLo,
                                    indexHi,
                                    indexMax
                            );
                        } else {
                            mergeStruct = createTempPartition(
                                    path,
                                    indexLo,
                                    indexHi,
                                    indexMax,
                                    dataIndexMax,
                                    1,
                                    fd < 0,
                                    timestampIndex,
                                    fd
                            );
                        }
                        try {

                            switch (prefixType) {
                                case OO_BLOCK_OO:
                                    LOG.info().$("copy ooo prefix set [from=").$(prefixLo).$(", to=").$(prefixHi).$(']').$();
                                    copyOutOfOrderData(prefixLo, prefixHi, mergeStruct, timestampIndex);
                                    break;
                                case OO_BLOCK_DATA:
                                    LOG.info().$("copy data prefix set [from=").$(prefixLo).$(", to=").$(prefixHi).$(']').$();
                                    copyPartitionData(prefixLo, prefixHi, mergeStruct);
                                    break;
                                default:
                                    break;
                            }

                            switch (mergeType) {
                                case OO_BLOCK_MERGE:
                                    LOG.info()
                                            .$("merge data2 + ooo2 [dataFrom=").$(mergeDataLo)
                                            .$(", dataTo=").$(mergeDataHi)
                                            .$(", oooFrom=").$(mergeOOOLo)
                                            .$(", oooTo=").$(mergeOOOHi)
                                            .$(']').$();
                                    mergeOOAndShuffleColumns(timestampIndex, mergedTimestamps, mergeDataLo, mergeDataHi, mergeOOOLo, mergeOOOHi, mergeStruct);
                                    break;
                                case OO_BLOCK_DATA:
                                    LOG.info().$("copy data middle set [from=").$(mergeDataLo).$(", to=").$(mergeDataHi).$(']').$();
                                    copyPartitionData(mergeDataLo, mergeDataHi, mergeStruct);
                                    break;
                                default:
                                    break;
                            }

                            switch (suffixType) {
                                case OO_BLOCK_OO:
                                    LOG.info().$("copy ooo suffix set [from=").$(suffixLo).$(", to=").$(suffixHi).$(']').$();
                                    copyOutOfOrderData(suffixLo, suffixHi, mergeStruct, timestampIndex);
                                    break;
                                case OO_BLOCK_DATA:
                                    LOG.info().$("copy data suffix set [from=").$(suffixLo).$(", to=").$(suffixHi).$(']').$();
                                    copyPartitionData(suffixLo, suffixHi, mergeStruct);
                                    break;
                                default:
                                    break;
                            }

                            if (prefixType != OO_BLOCK_NONE || mergeType != OO_BLOCK_NONE) {
                                copyTempPartitionBack(mergeStruct);
                            }

                        } finally {
                            freeMergeStruct(mergeStruct);
                        }
                    } finally {
                        if (fd > 0) {
                            ff.close(fd);
                        }
                    }
                }

                final long partitionSize = dataIndexMax + indexHi - indexLo + 1;
                if (indexHi + 1 < indexMax) {

                    fixedRowCount += partitionSize;
                    if (partitionTimestampHi < ceilOfMaxTimestamp) {
                        fixedRowCount -= dataIndexMax;
                    }

                    // We just updated non-last partition. It is possible that this partition
                    // has already been updated by transaction or out of order logic was the only
                    // code that updated it. To resolve this fork we need to check if "txPendingPartitionSizes"
                    // contains timestamp of this partition. If it does - we don't increment "txPartitionCount"
                    // (it has been incremented before out-of-order logic kicked in) and
                    // we use partition size from "txPendingPartitionSizes" to subtract from "txPartitionCount"

                    boolean missing = true;
                    long x = txPendingPartitionSizes.getAppendOffset() / 16;
                    for (long l = 0; l < x; l++) {
                        long ts = txPendingPartitionSizes.getLong(l * 16 + Long.BYTES);
                        if (ts == partitionTimestampHi) {
                            fixedRowCount -= txPendingPartitionSizes.getLong(l * 16);
                            txPendingPartitionSizes.putLong(l * 16, partitionSize);
                            missing = false;
                            break;
                        }
                    }

                    if (missing) {
                        txPartitionCount++;
                        txPendingPartitionSizes.putLong128(partitionSize, partitionTimestampHi);
                    }

                    indexLo = indexHi + 1;
                } else {
                    transientRowCount = partitionSize;
                    // Compute max timestamp as maximum of out of order data and
                    // data in existing partition.
                    // When partition is new, the data timestamp is MIN_LONG
                    maxTimestamp = Math.max(getTimestampIndexValue(mergedTimestamps, indexHi), dataTimestampHi);
                    minTimestamp = Math.min(minTimestamp, ooTimestampMin);
                    break;
                }
            }
        } finally {
            path.trimTo(rootLen);
            this.mergeRowCount = 0;
        }
    }

    private void mergeShuffle(
            long[] mergeStruct,
            long dataOOMergeIndex,
            long count,
            int columnIndex,
            int shl,
            MergeShuffleOutOfOrderDataInternal shuffleFunc
    ) {
        final long offset = MergeStruct.getDestFixedAppendOffset(mergeStruct, columnIndex);
        shuffleFunc.shuffle(
                MergeStruct.getSrcFixedAddress(mergeStruct, columnIndex),
                mergeColumns.getQuick(getPrimaryColumnIndex(columnIndex)).addressOf(0),
                MergeStruct.getDestFixedAddress(mergeStruct, columnIndex) + offset,
                dataOOMergeIndex,
                count
        );
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, offset + (count << shl));
    }

    private long mergeTimestampAndOutOfOrder(
            int timestampIndex,
            long mergedTimestamps,
            long mergeDataLo,
            long mergeDataHi,
            long mergeOOOLo,
            long mergeOOOHi,
            long[] mergeStruct
    ) {
        // Create "index" for existing timestamp column. When we reshuffle timestamps during merge we will
        // have to go back and find data rows we need to move accordingly
        final long indexSize = (mergeDataHi - mergeDataLo + 1) * TIMESTAMP_MERGE_ENTRY_BYTES;
        final long src = MergeStruct.getSrcFixedAddress(mergeStruct, timestampIndex);
        final long indexStruct = Unsafe.malloc(TIMESTAMP_MERGE_ENTRY_BYTES * 2);
        try {
            long index = Unsafe.malloc(indexSize);
            try {
                for (long l = mergeDataLo; l <= mergeDataHi; l++) {
                    Unsafe.getUnsafe().putLong(index + (l - mergeDataLo) * TIMESTAMP_MERGE_ENTRY_BYTES, Unsafe.getUnsafe().getLong(src + l * Long.BYTES));
                    Unsafe.getUnsafe().putLong(index + (l - mergeDataLo) * TIMESTAMP_MERGE_ENTRY_BYTES + Long.BYTES, l | (1L << 63));
                }

                Unsafe.getUnsafe().putLong(indexStruct, index);
                Unsafe.getUnsafe().putLong(indexStruct + Long.BYTES, mergeDataHi - mergeDataLo + 1);
                Unsafe.getUnsafe().putLong(indexStruct + 2 * Long.BYTES, mergedTimestamps + mergeOOOLo * 16);
                Unsafe.getUnsafe().putLong(indexStruct + 3 * Long.BYTES, mergeOOOHi - mergeOOOLo + 1);

                return Vect.mergeLongIndexesAsc(indexStruct, 2);
            } finally {
                Unsafe.free(index, indexSize);
            }
        } finally {
            Unsafe.free(indexStruct, TIMESTAMP_MERGE_ENTRY_BYTES * 2);
        }
    }

    private void mergeTimestampSetter(long timestamp) {
        timestampMergeMem.putLong(timestamp);
        timestampMergeMem.putLong(mergeRowCount++);
    }

    private long openAppend(LPSZ name) {
        long fd = ff.openAppend(name);
        if (fd == -1) {
            throw CairoException.instance(Os.errno()).put("Cannot open for append: ").put(name);
        }
        return fd;
    }

    private void openColumnFiles(CharSequence name, int i, int plen) {
        AppendMemory mem1 = getPrimaryColumn(i);
        AppendMemory mem2 = getSecondaryColumn(i);

        mem1.of(ff, dFile(path.trimTo(plen), name), ff.getMapPageSize());

        if (mem2 != null) {
            mem2.of(ff, iFile(path.trimTo(plen), name), ff.getMapPageSize());
        }

        path.trimTo(plen);
    }

    private void openFirstPartition(long timestamp) {
        openPartition(repairDataGaps(timestamp));
        setAppendPosition(transientRowCount);
        if (performRecovery) {
            performRecovery();
        }
        txPartitionCount = 1;
    }

    private void openMergePartition() {
        for (int i = 0; i < columnCount; i++) {
            ContiguousVirtualMemory mem1 = mergeColumns.getQuick(getPrimaryColumnIndex(i));
            mem1.clear();
            ContiguousVirtualMemory mem2 = mergeColumns.getQuick(getSecondaryColumnIndex(i));
            if (mem2 != null) {
                mem2.clear();
            }
        }
        row.rowColumns = mergeColumns;
        LOG.info().$("switched partition to memory").$();
    }

    private void openMetaFile() {
        path.concat(META_FILE_NAME).$();
        try {
            metaMem.of(ff, path, ff.getPageSize(), ff.length(path));
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openNewColumnFiles(CharSequence name, boolean indexFlag, int indexValueBlockCapacity) {
        try {
            // open column files
            setStateForTimestamp(path, maxTimestamp, false);
            final int plen = path.length();
            final int columnIndex = columnCount - 1;

            // index must be created before column is initialised because
            // it uses primary column object as temporary tool
            if (indexFlag) {
                createIndexFiles(name, indexValueBlockCapacity, plen, true);
            }

            openColumnFiles(name, columnIndex, plen);
            if (transientRowCount > 0) {
                // write .top file
                writeColumnTop(name);
            }

            if (indexFlag) {
                ColumnIndexer indexer = indexers.getQuick(columnIndex);
                assert indexer != null;
                indexers.getQuick(columnIndex).configureFollowerAndWriter(configuration, path.trimTo(plen), name, getPrimaryColumn(columnIndex), transientRowCount);
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
                    // because we are reusing AppendMemory object from columns list
                    createIndexFiles(name, metadata.getIndexValueBlockCapacity(i), plen, transientRowCount < 1);
                }

                openColumnFiles(name, i, plen);
                columnTop = readColumnTop(ff, path, name, plen, tempMem8b);
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

    private ReadWriteMemory openTxnFile() {
        try {
            if (ff.exists(path.concat(TXN_FILE_NAME).$())) {
                return new ReadWriteMemory(ff, path, ff.getPageSize());
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);

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

    private void prepareAppendColumn(AppendMemory mem, long[] mergeStruct, int columnIndex, long indexMergeSize) {
        final long fd = -mem.getFd();
        final long offset = mem.getAppendOffset();
        MergeStruct.setDestFixedFd(mergeStruct, columnIndex, fd);
        long indexMergeAddr = mapReadWriteOrFail(ff, null, -fd, 0, indexMergeSize + offset);
        MergeStruct.setDestFixedAddress(mergeStruct, columnIndex, indexMergeAddr);
        MergeStruct.setDestFixedAddressSize(mergeStruct, columnIndex, indexMergeSize + offset);
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, offset);
    }

    private long[] prepareAppendMemory(long indexLo, long indexHi, long indexMax) {
        long[] mergeStruct = new long[columnCount * MergeStruct.MERGE_STRUCT_ENTRY_SIZE];

        for (int i = 0; i < columnCount; i++) {
            ContiguousVirtualMemory mem = mergeColumns.getQuick(getPrimaryColumnIndex(i));
            ContiguousVirtualMemory mem2;
            final int columnType = metadata.getColumnType(i);

            long lo;
            long hi;
            switch (columnType) {
                case ColumnType.BINARY:
                case ColumnType.STRING:
                    mem2 = mergeColumns.getQuick(getSecondaryColumnIndex(i));
                    lo = mem2.getLong(indexLo * Long.BYTES);
                    if (indexHi == indexMax - 1) {
                        hi = mem.getAppendOffset();
                    } else {
                        hi = mem2.getLong((indexHi + 1) * Long.BYTES);
                    }

                    final long dataSize = (hi - lo);
                    prepareAppendColumn(
                            columns.getQuick(getSecondaryColumnIndex(i)),
                            mergeStruct,
                            i,
                            (indexHi - indexLo + 1) << ColumnType.pow2SizeOf(ColumnType.LONG)
                    );

                    final AppendMemory dataMem = columns.getQuick(getPrimaryColumnIndex(i));
                    final long dataMergeFd = -dataMem.getFd();
                    final long dataOffset = dataMem.getAppendOffset();
                    MergeStruct.setDestVarFd(mergeStruct, i, dataMergeFd);
                    final long dataMergeAddr = mapReadWriteOrFail(ff, null, -dataMergeFd, 0, dataSize + dataOffset);
                    MergeStruct.getDestVarAddress(mergeStruct, i, dataMergeAddr);
                    MergeStruct.setDestVarAddressSize(mergeStruct, i, dataSize + dataOffset);
                    MergeStruct.setDestVarAppendOffset(mergeStruct, i, dataOffset);
                    break;
                default:
                    prepareAppendColumn(
                            columns.getQuick(getPrimaryColumnIndex(i)),
                            mergeStruct,
                            i,
                            (indexHi - indexLo + 1) << ColumnType.pow2SizeOf(columnType)
                    );
                    break;
            }
        }
        return mergeStruct;
    }

    private void purgeUnusedPartitions() {
        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirsNewerThan(maxTimestamp);
        }
    }

    private long readTodoTaskCode() {
        try {
            if (ff.exists(path.concat(TODO_FILE_NAME).$())) {
                long todoFd = ff.openRO(path);
                if (todoFd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open *todo*: ").put(path);
                }
                long len = ff.read(todoFd, tempMem8b, 8, 0);
                ff.close(todoFd);
                if (len != 8L) {
                    LOG.info().$("Cannot read *todo* code. File seems to be truncated. Ignoring. [file=").$(path).$(']').$();
                    return -1;
                }
                return Unsafe.getUnsafe().getLong(tempMem8b);
            }
            return -1;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void recoverFromMetaRenameFailure(CharSequence columnName) {
        openMetaFile();
    }

    private void recoverFromSwapRenameFailure(CharSequence columnName) {
        recoverFrommTodoWriteFailure(columnName);
        removeTodoFile();
    }

    private void recoverFromSymbolMapWriterFailure(CharSequence columnName) {
        removeSymbolMapFilesQuiet(columnName);
        removeMetaFile();
        recoverFromSwapRenameFailure(columnName);
    }

    private void recoverFrommTodoWriteFailure(CharSequence columnName) {
        restoreMetaFrom(META_PREV_FILE_NAME, metaPrevIndex);
        openMetaFile();
    }

    private void recoverOpenColumnFailure(CharSequence columnName) {
        removeMetaFile();
        removeLastColumn();
        recoverFromSwapRenameFailure(columnName);
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
        Misc.free(mergeColumns.getQuick(pi));
        Misc.free(mergeColumns.getQuick(si));
        mergeColumns.remove(pi);
        mergeColumns.remove(pi);

        columnTops.removeIndex(columnIndex);
        nullers.remove(columnIndex);
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
                nameOffset += ContiguousVirtualMemory.getStorageLength(columnName);
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

    private void removePartitionDirsNewerThan(long timestamp) {
        if (timestamp > Long.MIN_VALUE) {
            LOG.info().$("purging [newerThen=").$ts(timestamp).$(", path=").$(path.$()).$(']').$();
        } else {
            LOG.info().$("cleaning [path=").$(path.$()).$(']').$();
        }
        try {
            ff.iterateDir(path.$(), (pName, type) -> {
                path.trimTo(rootLen);
                path.concat(pName).$();
                nativeLPSZ.of(pName);
                if (IGNORED_FILES.excludes(nativeLPSZ) && type == Files.DT_DIR) {
                    try {
                        long dirTimestamp = partitionDirFmt.parse(nativeLPSZ, null);
                        if (dirTimestamp <= timestamp) {
                            return;
                        }
                    } catch (NumericException ignore) {
                        // not a date?
                        // ignore exception and remove directory
                    }
                    if (ff.rmdir(path)) {
                        LOG.info().$("removing partition dir: ").$(path).$();
                    } else {
                        LOG.error().$("cannot remove: ").$(path).$(" [errno=").$(ff.errno()).$(']').$();
                    }
                }
            });
        } finally {
            path.trimTo(rootLen);
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
            denseSymbolMapWriters.remove(writer);
            Misc.free(writer);
        }
    }

    private void removeTodoFile() {
        try {
            if (!ff.remove(path.concat(TODO_FILE_NAME).$())) {
                throw CairoException.instance(Os.errno()).put("Recovery operation completed successfully but I cannot remove todo file: ").put(path).put(". Please remove manually before opening table again,");
            }
        } finally {
            path.trimTo(rootLen);
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
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i);
            }

            long nameOffset = getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                nameOffset += ContiguousVirtualMemory.getStorageLength(columnName);

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
        if (maxTimestamp != Numbers.LONG_NaN && partitionBy != PartitionBy.NONE) {
            long actualSize = 0;
            long lastTimestamp = -1;
            long transientRowCount = this.transientRowCount;
            long maxTimestamp = this.maxTimestamp;
            try {
                final long tsLimit = timestampFloorMethod.floor(this.maxTimestamp);
                for (long ts = minTimestamp; ts < tsLimit; ts = timestampAddMethod.calculate(ts, 1)) {
                    path.trimTo(rootLen);
                    setStateForTimestamp(path, ts, false);
                    int p = path.length();
                    if (ff.exists(path.concat(ARCHIVE_FILE_NAME).$())) {
                        actualSize += TableUtils.readLongAtOffset(ff, path, tempMem8b, 0);
                        lastTimestamp = ts;
                    } else {
                        if (removedPartitions.excludes(ts)) {
                            LOG.info().$("missing partition [name=").$(path.trimTo(p).$()).$(']').$();
                        }
                    }
                }

                if (lastTimestamp > -1) {
                    path.trimTo(rootLen);
                    setStateForTimestamp(path, tsLimit, false);
                    if (!ff.exists(path.$())) {
                        LOG.error().$("last partition does not exist [name=").$(path).$(']').$();

                        // ok, create last partition we discovered the active
                        // 1. read its size
                        path.trimTo(rootLen);
                        setStateForTimestamp(path, lastTimestamp, false);
                        int p = path.length();
                        transientRowCount = TableUtils.readLongAtOffset(ff, path.concat(ARCHIVE_FILE_NAME).$(), tempMem8b, 0);

                        // 2. read max timestamp
                        TableUtils.dFile(path.trimTo(p), metadata.getColumnName(metadata.getTimestampIndex()));
                        maxTimestamp = TableUtils.readLongAtOffset(ff, path, tempMem8b, (transientRowCount - 1) * Long.BYTES);
                        actualSize -= transientRowCount;
                        LOG.info()
                                .$("updated active partition [name=").$(path.trimTo(p).$())
                                .$(", maxTimestamp=").$ts(maxTimestamp)
                                .$(", transientRowCount=").$(transientRowCount)
                                .$(", fixedRowCount=").$(fixedRowCount)
                                .$(']').$();
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }

            final long expectedSize = txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
            if (expectedSize != actualSize || maxTimestamp != this.maxTimestamp) {
                LOG.info()
                        .$("actual table size has been adjusted [name=`").utf8(name).$('`')
                        .$(", expectedFixedSize=").$(expectedSize)
                        .$(", actualFixedSize=").$(actualSize)
                        .$(']').$();

                long txn = txMem.getLong(TX_OFFSET_TXN) + 1;
                txMem.putLong(TX_OFFSET_TXN, txn);
                Unsafe.getUnsafe().storeFence();

                txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT, actualSize);
                if (this.maxTimestamp != maxTimestamp) {
                    txMem.putLong(TX_OFFSET_MAX_TIMESTAMP, maxTimestamp);
                    txMem.putLong(TX_OFFSET_TRANSIENT_ROW_COUNT, transientRowCount);
                }
                Unsafe.getUnsafe().storeFence();

                // txn check
                txMem.putLong(TX_OFFSET_TXN_CHECK, txn);

                fixedRowCount = actualSize;
                this.maxTimestamp = maxTimestamp;
                this.transientRowCount = transientRowCount;
                this.txn = txn;
                return this.maxTimestamp;
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
                    throw CairoException.instance(Os.errno()).put("Repair failed. Cannot replace ").put(other);
                }

                if (!ff.rename(path, other)) {
                    throw CairoException.instance(Os.errno()).put("Repair failed. Cannot rename ").put(path).put(" -> ").put(other);
                }
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }

        removeTodoFile();
    }

    private void repairTruncate() {
        LOG.info().$("repairing abnormally terminated truncate on ").$(path).$();
        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirectories();
        }
        resetTxn(
                txMem,
                metadata.getSymbolMapCount(),
                txMem.getLong(TX_OFFSET_TXN) + 1,
                txMem.getLong(TX_OFFSET_DATA_VERSION) + 1);
        removeTodoFile();
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
        final long maxRow = transientRowCount - 1;
        for (int i = 0, n = denseIndexers.size(); i < n; i++) {
            ColumnIndexer indexer = denseIndexers.getQuick(i);
            LOG.info().$("recovering index [fd=").$(indexer.getFd()).$(']').$();
            indexer.rollback(maxRow);
        }
    }

    private void rollbackSymbolTables() {
        int expectedMapWriters = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT);
        for (int i = 0; i < expectedMapWriters; i++) {
            denseSymbolMapWriters.getQuick(i).rollback(txMem.getInt(getSymbolWriterIndexOffset(i)));
        }
    }

    private void runFragile(FragileCode fragile, CharSequence columnName, CairoException e) {
        try {
            fragile.run(columnName);
        } catch (CairoException err) {
            LOG.error().$("DOUBLE ERROR: 1st: '").$((Sinkable) e).$('\'').$();
            throwDistressException(err);
        }
        throw e;
    }

    private void setAppendPosition(final long position) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            setColumnSize(ff, getPrimaryColumn(i), getSecondaryColumn(i), getColumnType(metaMem, i), position - columnTops.getQuick(i), tempMem8b);
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
        int y, m, d;
        boolean leap;
        path.put(Files.SEPARATOR);
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

                if (updatePartitionInterval) {
                    partitionHi =
                            Timestamps.yearMicros(y, leap)
                                    + Timestamps.monthOfYearMicros(m, leap)
                                    + (d - 1) * Timestamps.DAY_MICROS + 24 * Timestamps.HOUR_MICROS - 1;
                }
                break;
            case PartitionBy.MONTH:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                m = Timestamps.getMonthOfYear(timestamp, y, leap);
                TimestampFormatUtils.append000(path, y);
                path.put('-');
                TimestampFormatUtils.append0(path, m);

                if (updatePartitionInterval) {
                    partitionHi =
                            Timestamps.yearMicros(y, leap)
                                    + Timestamps.monthOfYearMicros(m, leap)
                                    + Timestamps.getDaysPerMonth(m, leap) * 24L * Timestamps.HOUR_MICROS - 1;
                }
                break;
            case PartitionBy.YEAR:
                y = Timestamps.getYear(timestamp);
                leap = Timestamps.isLeapYear(y);
                TimestampFormatUtils.append000(path, y);
                if (updatePartitionInterval) {
                    partitionHi = Timestamps.addYear(Timestamps.yearMicros(y, leap), 1) - 1;
                }
                break;
            default:
                path.put(DEFAULT_PARTITION_NAME);
                partitionHi = Long.MAX_VALUE;
                break;
        }
    }

    private void shiftCopyFixedSizeColumnData(
            long shift,
            long src,
            long[] mergeStruct,
            int columnIndex,
            long srcLo,
            long srcHi
    ) {
        final int shl = ColumnType.pow2SizeOf(ColumnType.LONG);
        final long lo = srcLo << shl;
        final long hi = (srcHi + 1) << shl;
        final long slo = src + lo;
        final long destOffset = MergeStruct.getDestFixedAppendOffset(mergeStruct, columnIndex);
        final long dest = MergeStruct.getDestFixedAddress(mergeStruct, columnIndex) + destOffset;
        final long len = hi - lo;
        for (long o = 0; o < len; o += Long.BYTES) {
            Unsafe.getUnsafe().putLong(dest + o, Unsafe.getUnsafe().getLong(slo + o) - shift);
        }
        MergeStruct.setDestFixedAppendOffset(mergeStruct, columnIndex, destOffset + len);
    }

    private void shuffleFixedLengthValues(
            int columnIndex,
            long timestampIndex,
            long indexRowCount,
            final int shl,
            final ShuffleOutOfOrderDataInternal shuffleFunc
    ) {
        final ContiguousVirtualMemory mem = mergeColumns.getQuick(getPrimaryColumnIndex(columnIndex));
        final long src = mem.addressOf(0);
        final long srcSize = mem.getAllocatedSize();
        final long tgtSize = indexRowCount << shl;
        final long tgtDataAddr = Unsafe.malloc(tgtSize);
        shuffleFunc.shuffle(src, tgtDataAddr, timestampIndex, indexRowCount);
        mem.replacePage(tgtDataAddr, tgtSize);
        Unsafe.free(src, srcSize);
    }

    private void shuffleVarLenValues(int columnIndex, long timestampIndex, long indexRowCount) {
        final int primaryIndex = getPrimaryColumnIndex(columnIndex);
        final int secondaryIndex = getSecondaryColumnIndex(columnIndex);

        tmpShuffleIndex.jumpTo(0);
        tmpShuffleData.jumpTo(0);

        final ContiguousVirtualMemory dataMem = mergeColumns.getQuick(primaryIndex);
        final ContiguousVirtualMemory indexMem = mergeColumns.getQuick(secondaryIndex);
        final long dataSize = dataMem.getAppendOffset();
        // ensure we have enough memory allocated
        tmpShuffleData.jumpTo(dataSize);

        final long srcDataAddr = dataMem.addressOf(0);
        final long tgtDataAddr = tmpShuffleData.addressOf(0);

        long offset = 0;
        for (long l = 0; l < indexRowCount; l++) {
            long row = getTimestampIndexRow(timestampIndex, l);
            long o1 = indexMem.getLong(row * Long.BYTES);
            long o2 = row + 1 < indexRowCount ? indexMem.getLong((row + 1) * Long.BYTES) : dataSize;
            long len = o2 - o1;
            Unsafe.getUnsafe().copyMemory(srcDataAddr + o1, tgtDataAddr + offset, len);
            tmpShuffleIndex.putLong(l * Long.BYTES, offset);
            offset += len;
        }

        // ensure that append offsets of these memory instances match data length
        tmpShuffleData.jumpTo(offset);
        tmpShuffleIndex.jumpTo(indexRowCount * Long.BYTES);

        // now swap source and target memory
        mergeColumns.setQuick(primaryIndex, tmpShuffleData);
        mergeColumns.setQuick(secondaryIndex, tmpShuffleIndex);

        tmpShuffleIndex = indexMem;
        tmpShuffleData = dataMem;
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
        if (txPartitionCount++ > 0) {
            txPendingPartitionSizes.putLong128(transientRowCount, ceilMaxTimestamp());
        }
        fixedRowCount += transientRowCount;
        txPrevTransientRowCount = transientRowCount;
        transientRowCount = 0;
        openPartition(timestamp);
        setAppendPosition(0);
    }

    private void syncColumns(int commitMode) {
        final boolean async = commitMode == CommitMode.ASYNC;
        for (int i = 0; i < columnCount; i++) {
            columns.getQuick(i * 2).sync(async);
            final AppendMemory m2 = columns.getQuick(i * 2 + 1);
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
        if (indexCount == 0) {
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
        final long lo = txPartitionCount == 1 ? txPrevTransientRowCount : 0;
        final long hi = transientRowCount;
        if (indexCount > 1 && parallelIndexerEnabled && hi - lo > configuration.getParallelIndexThreshold()) {
            updateIndexesParallel(lo, hi);
        } else {
            updateIndexesSerially(lo, hi);
        }
    }

    private void updateMaxTimestamp(long timestamp) {
        this.prevMaxTimestamp = maxTimestamp;
        this.maxTimestamp = timestamp;
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
        long fd = openAppend(path.concat(name).put(".top").$());
        try {
            Unsafe.getUnsafe().putLong(tempMem8b, transientRowCount);
            if (ff.append(fd, tempMem8b, 8) != 8) {
                throw CairoException.instance(Os.errno()).put("Cannot append ").put(path);
            }
        } finally {
            ff.close(fd);
        }
    }

    private void writeRestoreMetaTodo(CharSequence columnName) {
        try {
            writeTodo(((long) metaPrevIndex << 8) | TODO_RESTORE_META);
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_TODO_WRITE_FAILURE, columnName, e);
        }
    }

    private void writeTodo(long code) {
        try {
            long fd = openAppend(path.concat(TODO_FILE_NAME).$());
            try {
                Unsafe.getUnsafe().putLong(tempMem8b, code);
                if (ff.append(fd, tempMem8b, 8) != 8) {
                    throw CairoException.instance(Os.errno()).put("Cannot write ").put(getTodoText(code)).put(" *todo*: ").put(path);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    @FunctionalInterface
    private interface ShuffleOutOfOrderDataInternal {
        void shuffle(long pSrc, long pDest, long pIndex, long count);
    }

    @FunctionalInterface
    private interface MergeShuffleOutOfOrderDataInternal {
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

    private class OpenPartitionRowFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            if (maxTimestamp != Long.MIN_VALUE) {
                return (rowFunction = switchPartitionFunction).newRow(timestamp);
            }
            return getRowSlow(timestamp);
        }

        private Row getRowSlow(long timestamp) {
            minTimestamp = timestamp;
            openFirstPartition(timestamp);
            return (rowFunction = switchPartitionFunction).newRow(timestamp);
        }
    }

    private class NoPartitionFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            if (timestamp >= maxTimestamp) {
                updateMaxTimestamp(timestamp);
                return row;
            }
            throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
        }
    }

    private class MergePartitionFunction implements RowFunction {
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
            if (timestamp > partitionHi || timestamp < maxTimestamp) {
                return newRow0(timestamp);
            }
            updateMaxTimestamp(timestamp);
            return row;
        }

        @NotNull
        private Row newRow0(long timestamp) {
            if (timestamp < maxTimestamp) {
                LOG.info().$("out-of-order").$();
                // Before partition can be switched we need to index records
                // added so far. Index writers will start point to different
                // files after switch.
//                updateIndexes();

                // We need to store reference on partition so that archive
                // file can be created in appropriate directory.
                // For simplicity use partitionLo, which can be
                // translated to directory name when needed

//                if (txPartitionCount++ > 0) {
//                    txPendingPartitionSizes.putLong128(transientRowCount, maxTimestamp);
//                }
//                fixedRowCount += transientRowCount;
//                txPrevTransientRowCount = transientRowCount;
//                transientRowCount = 0;

                // todo: do we need this?
                TableWriter.this.transientRowCountBeforeOutOfOrder = TableWriter.this.transientRowCount;
                openMergePartition();
                TableWriter.this.mergeRowCount = 0;
                assert timestampMergeMem != null;
                timestampSetter = mergeTimestampMethodRef;
                timestampSetter.accept(timestamp);
                TableWriter.this.rowFunction = mergePartitionFunction;
                return row;
            }

            if (timestamp > partitionHi && partitionBy != PartitionBy.NONE) {
                switchPartition(timestamp);
            }

            updateMaxTimestamp(timestamp);
            return row;
        }
    }

    public class Row {
        private ObjList<? extends BigMem> rowColumns;

        // ISO 27001
        // todo: configure nullers for the merge columns
        public void append() {
            if ((masterRef & 1) != 0) {
                for (int i = 0; i < columnCount; i++) {
                    if (refs.getQuick(i) < masterRef) {
                        nullers.getQuick(i).run();
                    }
                }
                transientRowCount++;
                masterRef++;
                if (prevMinTimestamp == Long.MAX_VALUE) {
                    prevMinTimestamp = minTimestamp;
                }
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

        public void putTimestamp(int index, long value) {
            putLong(index, value);
        }

        public void putTimestamp(int index, CharSequence value) {
            // try UTC timestamp first (micro)
            long l;
            try {
                l = TimestampFormatUtils.parseTimestamp(value);
            } catch (NumericException e) {
                try {
                    l = TimestampFormatUtils.parseDateTime(value);
                } catch (NumericException numericException) {
                    throw CairoException.instance(0).put("could not convert to timestamp [value=").put(value).put(']');
                }
            }
            putTimestamp(index, l);
        }

        private BigMem getPrimaryColumn(int columnIndex) {
            return rowColumns.getQuick(getPrimaryColumnIndex(columnIndex));
        }

        private BigMem getSecondaryColumn(int columnIndex) {
            return rowColumns.getQuick(getSecondaryColumnIndex(columnIndex));
        }

        private void notNull(int index) {
            refs.setQuick(index, masterRef);
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
