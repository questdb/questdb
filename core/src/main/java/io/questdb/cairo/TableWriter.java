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

    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final CharSequenceHashSet IGNORED_FILES = new CharSequenceHashSet();
    private static final Runnable NOOP = () -> {
    };
    private final static RemoveFileLambda REMOVE_OR_LOG = TableWriter::removeFileAndOrLog;
    private final static RemoveFileLambda REMOVE_OR_EXCEPTION = TableWriter::removeOrException;
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
    private final VirtualMemory txPendingPartitionSizes;
    private final int partitionBy;
    private final RowFunction switchPartitionFunction = new SwitchPartitionRowFunction();
    private final RowFunction openPartitionFunction = new OpenPartitionRowFunction();
    private final RowFunction noPartitionFunction = new NoPartitionFunction();
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
            this.txPendingPartitionSizes = new VirtualMemory(ff.getPageSize());
            this.refs.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount * 2);
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
                    setStateForTimestamp(timestamp, true);
                } else {
                    setStateForTimestamp(0, false);
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

            setStateForTimestamp(timestamp, false);

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
                    readBytes(ff, mem1, buf, 8, offset, "Cannot read length, fd=");
                    len = Unsafe.getUnsafe().getLong(buf);
                    mem1.setSize(len == -1 ? offset + 8 : offset + len + 8);
                    mem2.setSize(actualPosition * 8);
                    break;
                case ColumnType.STRING:
                    assert mem2 != null;
                    readOffsetBytes(ff, mem2, actualPosition, buf);
                    offset = Unsafe.getUnsafe().getLong(buf);
                    readBytes(ff, mem1, buf, 4, offset, "Cannot read length, fd=");
                    len = Unsafe.getUnsafe().getInt(buf);
                    mem1.setSize(len == -1 ? offset + 4 : offset + len * 2 + 4);
                    mem2.setSize(actualPosition * 8);
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

    private static void readOffsetBytes(FilesFacade ff, AppendMemory mem, long position, long buf) {
        readBytes(ff, mem, buf, 8, (position - 1) * 8, "Cannot read offset, fd=");
    }

    private static void readBytes(FilesFacade ff, AppendMemory mem, long buf, int byteCount, long offset, CharSequence errorMsg) {
        if (ff.read(mem.getFd(), buf, byteCount, offset) != byteCount) {
            throw CairoException.instance(ff.errno()).put(errorMsg).put(mem.getFd()).put(", offset=").put(offset);
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
            nameOffset += VirtualMemory.getStorageLength(col);
        }
        return -1;
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

    private int addColumnToMeta(
            CharSequence name,
            int type,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean sequentialFlag) {
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
                nameOffset += VirtualMemory.getStorageLength(columnName);
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
                        setStateForTimestamp(maxTimestamp, false);
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
                setStateForTimestamp(partitionTimestamp, false);
                long fd = openAppend(path.concat(ARCHIVE_FILE_NAME).$());
                try {
                    int len = 8;
                    long o = offset;
                    while (len > 0) {
                        long l = Math.min(len, txPendingPartitionSizes.pageRemaining(o));
                        if (ff.write(fd, txPendingPartitionSizes.addressOf(o), l, 0) == l) {
                            len -= l;
                            o += l;
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
        switch (type) {
            case ColumnType.BINARY:
            case ColumnType.STRING:
                secondary = new AppendMemory();
                break;
            default:
                secondary = null;
                break;
        }
        columns.add(primary);
        columns.add(secondary);
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
                nameOffset += VirtualMemory.getStorageLength(columnName);
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
                nameOffset += VirtualMemory.getStorageLength(columnName);
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
        if (columns != null) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                AppendMemory m = columns.getQuick(i);
                if (m != null) {
                    m.close(truncate);
                }
            }
        }
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
            setStateForTimestamp(nextTimestamp, false);
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

                setStateForTimestamp(timestamp, true);

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
            setStateForTimestamp(maxTimestamp, false);
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
            setStateForTimestamp(timestamp, true);
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

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
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
        Misc.free(getPrimaryColumn(columnIndex));
        Misc.free(getSecondaryColumn(columnIndex));
        columns.remove(getSecondaryColumnIndex(columnIndex));
        columns.remove(getPrimaryColumnIndex(columnIndex));
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
                nameOffset += VirtualMemory.getStorageLength(columnName);
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
                    setStateForTimestamp(ts, false);
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
                    setStateForTimestamp(tsLimit, false);
                    if (!ff.exists(path.$())) {
                        LOG.error().$("last partition does not exist [name=").$(path).$(']').$();

                        // ok, create last partition we discovered the active
                        // 1. read its size
                        path.trimTo(rootLen);
                        setStateForTimestamp(lastTimestamp, false);
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
     * @param timestamp               to determine interval for
     * @param updatePartitionInterval flag indicating that partition interval partitionLo and
     *                                partitionHi have to be updated as well.
     */
    private void setStateForTimestamp(long timestamp, boolean updatePartitionInterval) {
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
            txPendingPartitionSizes.putLong128(transientRowCount, maxTimestamp);
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
