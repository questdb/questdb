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
import io.questdb.Metrics;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.NullMapWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.*;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.function.LongConsumer;

import static io.questdb.cairo.MapWriter.createSymbolMapFiles;
import static io.questdb.cairo.TableUtils.*;

public class WalWriter implements Closeable {
    private static final int ROW_ACTION_OPEN_PARTITION = 0;
    private static final int ROW_ACTION_NO_PARTITION = 1;
    private static final int ROW_ACTION_NO_TIMESTAMP = 2;
    private static final int ROW_ACTION_SWITCH_PARTITION = 4;
    private static final Log LOG = LogFactory.getLog(WalWriter.class);
    private static final CharSequenceHashSet IGNORED_FILES = new CharSequenceHashSet();
    private static final Runnable NOOP = () -> {
    };
    final ObjList<MemoryMA> columns;
    private final ObjList<MapWriter> symbolMapWriters;
    private final ObjList<MapWriter> denseSymbolMapWriters;
    private final Path path;
    private final Path other;
    private final Path walDPath;
    private final LongList rowValueIsNotNull = new LongList();
    private final LongList walDRowValueIsNotNull = new LongList();
    private final Row regularRow = new RowImpl();
    private final int walDRootLen;
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
    private final TxReader slaveTxReader;
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
    private final ObjList<Runnable> nullSetters;
    private final ObjList<Runnable> walDNullSetters;
    private final MemoryMARW todoMem = Vm.getMARWInstance();
    private final TxWriter txWriter;
    private final TxnScoreboard txnScoreboard;
    private final StringSink fileNameSink = new StringSink();
    private final FindVisitor removePartitionDirectories = this::removePartitionDirectories0;
    private final FindVisitor removePartitionDirsNotAttached = this::removePartitionDirsNotAttached;
    private final LongConsumer appendTimestampSetter;
    private final MemoryMR indexMem = Vm.getMRInstance();
    // Latest command sequence per command source.
    // Publisher source is identified by a long value
    private final ColumnVersionWriter columnVersionWriter;
    private final Metrics metrics;
    private Row row = regularRow;
    private long todoTxn;
    private long lockFd = -1;
    private LongConsumer timestampSetter;
    private int columnCount;
    private boolean avoidIndexOnCommit = false;
    private long partitionTimestampHi;
    private long masterRef = 0;
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
    private long lastPartitionTimestamp;
    private ObjList<? extends MemoryA> activeColumns;
    private ObjList<MemoryMA> walDColumns;
    private ObjList<Runnable> activeNullSetters;
    private int rowActon = ROW_ACTION_OPEN_PARTITION;
    private long committedMasterRef;
    // ILP related
    private double commitIntervalFraction;
    private long commitIntervalDefault;
    private long commitInterval;
    private long walDRowCounter;
    private long waldPartitionCounter = -1;

    public WalWriter(CairoConfiguration configuration, CharSequence tableName, Metrics metrics) {
        this(configuration, tableName, null, new MessageBusImpl(configuration), true, DefaultLifecycleManager.INSTANCE, configuration.getRoot(), metrics);
    }

    public WalWriter(CairoConfiguration configuration, CharSequence tableName, @NotNull MessageBus messageBus, Metrics metrics) {
        this(configuration, tableName, messageBus, true, DefaultLifecycleManager.INSTANCE, metrics);
    }

    public WalWriter(
            CairoConfiguration configuration,
            CharSequence tableName,
            @NotNull MessageBus messageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            Metrics metrics
    ) {
        this(configuration, tableName, messageBus, null, lock, lifecycleManager, configuration.getRoot(), metrics);
    }

    public WalWriter(
            CairoConfiguration configuration,
            CharSequence tableName,
            MessageBus messageBus,
            MessageBus ownMessageBus,
            boolean lock,
            LifecycleManager lifecycleManager,
            CharSequence root,
            Metrics metrics
    ) {
        LOG.info().$("open '").utf8(tableName).$('\'').$();
        this.configuration = configuration;
        this.metrics = metrics;
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
        this.path = new Path();
        this.path.of(root).concat(tableName);
        this.other = new Path().of(root).concat(tableName);
        this.walDPath = new Path().of(root).concat(tableName).concat("wal");
        this.walDRootLen = walDPath.length();
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
            this.columnVersionWriter = openColumnVersionFile(ff, path, rootLen);

            openMetaFile(ff, path, rootLen, metaMem);
            this.metadata = new TableWriterMetadata(metaMem);
            this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
            this.txWriter = new TxWriter(ff).ofRW(path, partitionBy);
            this.txnScoreboard = new TxnScoreboard(ff, configuration.getTxnScoreboardEntryCount()).ofRW(path.trimTo(rootLen));
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
            this.walDRowValueIsNotNull.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount * 2);
            this.activeColumns = columns;
            this.symbolMapWriters = new ObjList<>(columnCount);
            this.denseSymbolMapWriters = new ObjList<>(metadata.getSymbolMapCount());
            this.nullSetters = new ObjList<>(columnCount);
            this.walDNullSetters = new ObjList<>(columnCount);
            this.activeNullSetters = nullSetters;
            this.columnTops = new LongList(columnCount);
            this.partitionFloorMethod = PartitionBy.getPartitionFloorMethod(partitionBy);
            this.partitionCeilMethod = PartitionBy.getPartitionCeilMethod(partitionBy);
            if (PartitionBy.isPartitioned(partitionBy)) {
                partitionDirFmt = PartitionBy.getPartitionDirFormatMethod(partitionBy);
            } else {
                partitionDirFmt = null;
            }
            this.commitInterval = calculateCommitInterval();

            configureColumnMemory();
            this.walDColumns = new ObjList<>(columnCount * 2);
            configureWalDColumnMemory();

            openNewWalDPartition();
            configureWalDSymbolTable();
            configureTimestampSetter();
            this.appendTimestampSetter = timestampSetter;
            configureAppendPosition();
            purgeUnusedPartitions();
            clearTodoLog();
            this.slaveTxReader = new TxReader(ff);
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

        commit();

        long columnNameTxn = getTxn();
        LOG.info().$("adding column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("], name txn ").$(columnNameTxn).$(" to ").$(path).$();

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
                createSymbolMapWriter(name, columnNameTxn, symbolCapacity, symbolCacheFlag);
            } catch (CairoException e) {
                runFragile(RECOVER_FROM_SYMBOL_MAP_WRITER_FAILURE, name, e);
            }
        } else {
            // maintain sparse list of symbol writers
            symbolMapWriters.extendAndSet(columnCount, NullMapWriter.INSTANCE);
        }

        // add column objects
        configureColumn(type, isIndexed, columnCount);

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

        bumpStructureVersion();

        metadata.addColumn(name, configuration.getRandom().nextLong(), type, isIndexed, indexValueBlockCapacity, columnIndex);

        LOG.info().$("ADDED column '").utf8(name).$('[').$(ColumnType.nameOf(type)).$("], name txn ").$(columnNameTxn).$(" to ").$(path).$();
    }

    public void changeCacheFlag(int columnIndex, boolean cache) {
        checkDistressed();

        commit();

        MapWriter symbolMapWriter = getSymbolMapWriter(columnIndex);
        if (symbolMapWriter.isCached() != cache) {
            symbolMapWriter.updateCacheFlag(cache);
        } else {
            return;
        }
        updateMetaStructureVersion();
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
        throw CairoException.instance(0).put("column '").put(name).put("' does not exist");
    }

    public long getColumnNameTxn(long partitionTimestamp, int columnIndex) {
        return columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
    }

    public long getCommitInterval() {
        return commitInterval;
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

    public int getPartitionBy() {
        return partitionBy;
    }

    public int getPartitionCount() {
        return txWriter.getPartitionCount();
    }

    public long getPartitionTimestamp(int partitionIndex) {
        return txWriter.getPartitionTimestamp(partitionIndex);
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

    // todo: hide raw memory access from public interface when slave is able to send data over the network
    public long getRawTxnMemorySize() {
        return txWriter.unsafeGetRawMemorySize();
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

    public long getUncommittedRowCount() {
        return (masterRef - committedMasterRef) >> 1;
    }

    public long getCurrentWalDPartitionRowCount() {
        return walDRowCounter;
    }

    public boolean inTransaction() {
        return txWriter != null && txWriter.inTransaction();
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
                    throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order to non-partitioned table. Table=").put(path);
                }

                bumpMasterRef();
                updateMaxTimestamp(timestamp);
                break;
            case ROW_ACTION_NO_TIMESTAMP:
                bumpMasterRef();
                break;
        }
        txWriter.append();
        return row;
    }

    public Row newRow() {
        return newRow(0L);
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
        } catch (CairoException err) {
            throwDistressException(err);
        }

        bumpStructureVersion();

        metadata.removeColumn(index);
        if (timestamp) {
            metadata.setTimestampIndex(-1);
        } else if (timestampColumnName != null) {
            int timestampIndex2 = metadata.getColumnIndex(timestampColumnName);
            metadata.setTimestampIndex(timestampIndex2);
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
            long nextMinTimestamp = minTimestamp;
            if (timestamp == txWriter.getPartitionTimestamp(0)) {
                nextMinTimestamp = readMinTimestamp(txWriter.getPartitionTimestamp(1));
            }
            long partitionNameTxn = txWriter.getPartitionNameTxnByPartitionTimestamp(timestamp);
            txWriter.beginPartitionSizeUpdate();
            txWriter.removeAttachedPartitions(timestamp);
            txWriter.setMinTimestamp(nextMinTimestamp);
            txWriter.finishPartitionSizeUpdate(nextMinTimestamp, txWriter.getMaxTimestamp());
            txWriter.bumpTruncateVersion();
            txWriter.commit(defaultCommitMode, denseSymbolMapWriters);

            return true;
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
            openMetaFile(ff, path, rootLen, metaMem);

            // remove _todo
            clearTodoLog();

            // rename column files has to be done after _todo is removed
            renameColumnFiles(currentName, index, newName, type);
        } catch (CairoException err) {
            throwDistressException(err);
        }

        bumpStructureVersion();

        metadata.renameColumn(currentName, newName);

        if (index == metadata.getTimestampIndex()) {
            designatedTimestampColumnName = Chars.toString(newName);
        }

        LOG.info().$("RENAMED column '").utf8(currentName).$("' to '").utf8(newName).$("' from ").$(path).$();
    }

    public void rollback() {
        checkDistressed();
        if (inTransaction()) {
            try {
                LOG.info().$("tx rollback [name=").$(tableName).$(']').$();
                if ((masterRef & 1) != 0) {
                    masterRef++;
                }
                freeColumns(false);
                this.txWriter.unsafeLoadAll();
                rollbackSymbolTables();
                purgeUnusedPartitions();
                configureAppendPosition();
                LOG.info().$("tx rollback complete [name=").$(tableName).$(']').$();
                metrics.tableWriter().incrementRollbacks();
            } catch (Throwable e) {
                LOG.critical().$("could not perform rollback [name=").$(tableName).$(", msg=").$(e.getMessage()).$(']').$();
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
            commitInterval = calculateCommitInterval();
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
        return txWriter.getRowCount();
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
        columnVersionWriter.truncate();
        txWriter.truncate(columnVersionWriter.getVersion());
        row = regularRow;
        try {
            clearTodoLog();
        } catch (CairoException err) {
            throwDistressException(err);
        }

        LOG.info().$("truncated [name=").$(tableName).$(']').$();
    }

    public void updateCommitInterval(double commitIntervalFraction, long commitIntervalDefault) {
        this.commitIntervalFraction = commitIntervalFraction;
        this.commitIntervalDefault = commitIntervalDefault;
        this.commitInterval = calculateCommitInterval();
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
            LOG.critical().$("index error [fd=").$(indexer.getFd()).$(']').$('{').$((Sinkable) e).$('}').$();
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
            int columnType = getColumnType(metaMem, i); // Negative means deleted column
            if (columnType > 0 && Chars.equalsIgnoreCase(col, name)) {
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
                break;
            default:
                nullers.add(NOOP);
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

    private static void openWalDMetaFile(FilesFacade ff, Path path, int rootLen, MemoryMR metaMem) {
        path.concat(META_FILE_NAME).$();
        try {
            metaMem.smallFile(ff, path, MemoryTag.MMAP_TABLE_WALD_WRITER);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private static ColumnVersionWriter openColumnVersionFile(FilesFacade ff, Path path, int rootLen) {
        path.concat(COLUMN_VERSION_FILE_NAME).$();
        try {
            return new ColumnVersionWriter(ff, path, 0);
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void attachPartitionCheckFilesMatchFixedColumn(FilesFacade ff, Path path, int columnType, long partitionSize, String columnName, long columnNameTxn) {
        TableUtils.dFile(path, columnName, columnNameTxn);
        if (ff.exists(path.$())) {
            long fileSize = ff.length(path);
            if (fileSize < partitionSize << ColumnType.pow2SizeOf(columnType)) {
                throw CairoException.instance(0)
                        .put("Column file is too small. ")
                        .put("Partition files inconsistent [file=")
                        .put(path)
                        .put(", expectedSize=")
                        .put(partitionSize << ColumnType.pow2SizeOf(columnType))
                        .put(", actual=")
                        .put(fileSize)
                        .put(']');
            }
            return;
        }
        throw CairoException.instance(0).put("Column file does not exist [path=").put(path).put(']');
    }

    private void attachPartitionCheckFilesMatchMetadata(FilesFacade ff, Path path, RecordMetadata metadata, long partitionSize) throws CairoException {
        // for each column, check that file exists in the partition folder
        int rootLen = path.length();
        for (int columnIndex = 0, size = metadata.getColumnCount(); columnIndex < size; columnIndex++) {
            try {
                int columnType = metadata.getColumnType(columnIndex);
                final String columnName = metadata.getColumnName(columnIndex);
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
                    case ColumnType.LONG256:
                    case ColumnType.GEOBYTE:
                    case ColumnType.GEOSHORT:
                    case ColumnType.GEOINT:
                    case ColumnType.GEOLONG:
                        attachPartitionCheckFilesMatchFixedColumn(ff, path, columnType, partitionSize, columnName, columnNameTxn);
                        break;
                    case ColumnType.STRING:
                    case ColumnType.BINARY:
                        attachPartitionCheckFilesMatchVarLenColumn(ff, path, partitionSize, columnName, columnNameTxn);
                        break;
                    case ColumnType.SYMBOL:
                        attachPartitionCheckSymbolColumn(ff, path, columnIndex, partitionSize, columnName, columnNameTxn);
                        break;
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void attachPartitionCheckFilesMatchVarLenColumn(FilesFacade ff, Path path, long partitionSize, String columnName, long columnNameTxn) {
        int pathLen = path.length();
        TableUtils.dFile(path, columnName, columnNameTxn);
        long dataLength = ff.length(path.$());

        path.trimTo(pathLen);
        TableUtils.iFile(path, columnName, columnNameTxn);

        if (dataLength >= 0 && ff.exists(path.$())) {
            int typeSize = Long.BYTES;
            long indexFd = openRO(ff, path, LOG);
            try {
                long fileSize = ff.length(indexFd);
                long expectedFileSize = (partitionSize + 1) * typeSize;
                if (fileSize < expectedFileSize) {
                    throw CairoException.instance(0)
                            .put("Column file is too small. ")
                            .put("Partition files inconsistent [file=")
                            .put(path)
                            .put(",expectedSize=")
                            .put(expectedFileSize)
                            .put(",actual=")
                            .put(fileSize)
                            .put(']');
                }

                long mappedAddr = mapRO(ff, indexFd, expectedFileSize, MemoryTag.MMAP_DEFAULT);
                try {
                    long prevDataAddress = dataLength - 4;
                    for (long offset = partitionSize * typeSize; offset >= 0; offset -= typeSize) {
                        long dataAddress = Unsafe.getUnsafe().getLong(mappedAddr + offset);
                        if (dataAddress < 0 || dataAddress > dataLength) {
                            throw CairoException.instance(0).put("Variable size column has invalid data address value [path=").put(path)
                                    .put(", indexOffset=").put(offset)
                                    .put(", dataAddress=").put(dataAddress)
                                    .put(", dataFileSize=").put(dataLength)
                                    .put(']');
                        }

                        // Check that addresses are monotonic
                        if (dataAddress >= prevDataAddress) {
                            throw CairoException.instance(0).put("Variable size column has invalid data address value [path=").put(path)
                                    .put(", indexOffset=").put(offset)
                                    .put(", dataAddress=").put(dataAddress)
                                    .put(", prevDataAddress=").put(prevDataAddress)
                                    .put(", dataFileSize=").put(dataLength)
                                    .put(']');
                        }
                        prevDataAddress = dataAddress;
                    }
                    return;
                } finally {
                    ff.munmap(mappedAddr, expectedFileSize, MemoryTag.MMAP_DEFAULT);
                }
            } finally {
                ff.close(indexFd);
            }
        }
        throw CairoException.instance(0).put("Column file does not exist [path=").put(path).put(']');
    }

    private void attachPartitionCheckSymbolColumn(FilesFacade ff, Path path, int columnIndex, long partitionSize, String columnName, long columnNameTxn) {
        int pathLen = path.length();
        TableUtils.dFile(path, columnName, columnNameTxn);
        long fd = openRO(ff, path.$(), LOG);
        try {
            long fileSize = ff.length(fd);
            int typeSize = Integer.BYTES;
            long expectedSize = partitionSize * typeSize;
            if (fileSize < expectedSize) {
                throw CairoException.instance(0)
                        .put("Column file is too small. ")
                        .put("Partition files inconsistent [file=")
                        .put(path)
                        .put(", expectedSize=")
                        .put(expectedSize)
                        .put(", actual=")
                        .put(fileSize)
                        .put(']');
            }

            long address = mapRO(ff, fd, fileSize, MemoryTag.MMAP_DEFAULT);
            try {
                int maxKey = Vect.maxInt(address, partitionSize);
                int symbolValues = symbolMapWriters.getQuick(columnIndex).getSymbolCount();
                if (maxKey >= symbolValues) {
                    throw CairoException.instance(0)
                            .put("Symbol file does not match symbol column [file=")
                            .put(path)
                            .put(", key=")
                            .put(maxKey)
                            .put(", columnKeys=")
                            .put(symbolValues)
                            .put(']');
                }
                int minKey = Vect.minInt(address, partitionSize);
                if (minKey < 0) {
                    throw CairoException.instance(0)
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
                BitmapIndexUtils.valueFileName(path.trimTo(pathLen), columnName, columnNameTxn);
                if (!ff.exists(path.$())) {
                    throw CairoException.instance(0)
                            .put("Symbol index value file does not exist [file=")
                            .put(path)
                            .put(']');
                }
                BitmapIndexUtils.keyFileName(path.trimTo(pathLen), columnName, columnNameTxn);
                if (!ff.exists(path.$())) {
                    throw CairoException.instance(0)
                            .put("Symbol index key file does not exist [file=")
                            .put(path)
                            .put(']');
                }
            }
        } finally {
            ff.close(fd);
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
            copyVersionAndLagValues(ddlMem, true);
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i, false, ddlMem);
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

    private void bumpStructureVersion() {
        columnVersionWriter.commit();
        txWriter.setColumnVersion(columnVersionWriter.getVersion());
        txWriter.bumpStructureVersion(this.denseSymbolMapWriters);
        assert txWriter.getStructureVersion() == metadata.getStructureVersion();
    }

    private long calculateCommitInterval() {
        long commitIntervalMicros = (long) (metadata.getCommitLag() * commitIntervalFraction);
        return commitIntervalMicros > 0 ? commitIntervalMicros / 1000 : commitIntervalDefault;
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
    }

    void closeActivePartition(long size) {
        for (int i = 0; i < columnCount; i++) {
            // stop calculating oversize as soon as we find first over-sized column
            setColumnSize(i, size, false);
            Misc.free(getPrimaryColumn(i));
            Misc.free(getSecondaryColumn(i));
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

        if ((masterRef & 1) != 0) {
            rowCancel();
        }

        if (inTransaction()) {

            if (commitMode != CommitMode.NOSYNC) {
                syncColumns(commitMode);
            }

            final long committedRowCount = txWriter.unsafeCommittedFixedRowCount() + txWriter.unsafeCommittedTransientRowCount();
            final long rowsAdded = txWriter.getRowCount() - committedRowCount;

            updateIndexes();
            columnVersionWriter.commit();
            txWriter.setColumnVersion(columnVersionWriter.getVersion());
            txWriter.commit(commitMode, this.denseSymbolMapWriters);

            // Bookmark masterRef to track how many rows is in uncommitted state
            this.committedMasterRef = masterRef;

            metrics.tableWriter().incrementCommits();
            metrics.tableWriter().addCommittedRows(rowsAdded);
        }
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

    private void configureColumn(int type, boolean indexFlag, int index) {
        final MemoryMA primary;
        final MemoryMA secondary;

        if (type > 0) {
            primary = Vm.getMAInstance();

            switch (ColumnType.tagOf(type)) {
                case ColumnType.BINARY:
                case ColumnType.STRING:
                    secondary = Vm.getMAInstance();
                    break;
                default:
                    secondary = null;
                    break;
            }
        } else {
            primary = secondary = NullMemory.INSTANCE;
        }

        int baseIndex = getPrimaryColumnIndex(index);
        columns.extendAndSet(baseIndex, primary);
        columns.extendAndSet(baseIndex + 1, secondary);
        configureNullSetters(nullSetters, type, primary, secondary);
        rowValueIsNotNull.add(0);
    }

    private void configureWalDColumn(int type, int index) {
        final MemoryMA primary;
        final MemoryMA secondary;

        if (type > 0) {
            primary = Vm.getMAInstance();

            switch (ColumnType.tagOf(type)) {
                case ColumnType.BINARY:
                case ColumnType.STRING:
                    secondary = Vm.getMAInstance();
                    break;
                default:
                    secondary = null;
                    break;
            }
        } else {
            primary = secondary = NullMemory.INSTANCE;
        }

        int baseIndex = getPrimaryColumnIndex(index);
        walDColumns.extendAndSet(baseIndex, primary);
        walDColumns.extendAndSet(baseIndex + 1, secondary);
        configureNullSetters(walDNullSetters, type, primary, secondary);
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
    }

    private void configureWalDColumnMemory() {
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            configureWalDColumn(type, i);
        }
    }

    private void configureWalDSymbolTable() {
        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            if (ColumnType.isSymbol(type)) {
                final int symbolIndex = denseSymbolMapWriters.size();
                long columnNameTxn = columnVersionWriter.getDefaultColumnNameTxn(i);
                SymbolMapWriter symbolMapWriter = new SymbolMapWriter(
                        configuration,
                        walDPath.trimTo(walDRootLen),
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
    }

    private void configureTimestampSetter() {
        int index = metadata.getTimestampIndex();
        if (index == -1) {
            timestampSetter = value -> {
            };
        } else {
            nullSetters.setQuick(index, NOOP);
            timestampSetter = getPrimaryColumn(index)::putLong;
        }
    }

    private int copyMetadataAndSetIndexed(int columnIndex, int indexValueBlockSize) {
        try {
            int index = openMetaSwapFile(ff, ddlMem, path, rootLen, configuration.getMaxSwapFileCount());
            int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            ddlMem.putInt(columnCount);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            copyVersionAndLagValues(ddlMem, true);
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);
            for (int i = 0; i < columnCount; i++) {
                if (i != columnIndex) {
                    writeColumnEntry(i, false, ddlMem);
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
            long nameOffset = copyMetadataTo(ddlMem, true);
            this.metaSwapIndex = index;
            return nameOffset;
        } finally {
            ddlMem.close();
        }
    }

    private long copyMetadataTo(MemoryMAR target, boolean updateVersion) {
        int columnCount = metaMem.getInt(META_OFFSET_COUNT);

        target.putInt(columnCount);
        target.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
        target.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
        copyVersionAndLagValues(target, updateVersion);
        target.jumpTo(META_OFFSET_COLUMN_TYPES);
        for (int i = 0; i < columnCount; i++) {
            writeColumnEntry(i, false, target);
        }

        long nameOffset = getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence columnName = metaMem.getStr(nameOffset);
            target.putStr(columnName);
            nameOffset += Vm.getStorageLength(columnName);
        }
        return nameOffset;
    }

    private void copyVersionAndLagValues(MemoryMAR target, boolean updateVersion) {
        target.putInt(ColumnType.VERSION);
        target.putInt(metaMem.getInt(META_OFFSET_TABLE_ID));
        target.putInt(metaMem.getInt(META_OFFSET_MAX_UNCOMMITTED_ROWS));
        target.putLong(metaMem.getLong(META_OFFSET_COMMIT_LAG));
        long structureVersion = txWriter.getStructureVersion();
        if (updateVersion) {
            structureVersion++;
        }
        target.putLong(structureVersion);
        if (updateVersion) {
            metadata.setStructureVersion(structureVersion);
        }
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
            BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn);

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
            if (!ff.touch(BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn))) {
                LOG.error().$("could not create index [name=").$(path).$(']').$();
                throw CairoException.instance(ff.errno()).put("could not create index [name=").put(path).put(']');
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

    private void doClose(boolean truncate) {
        boolean tx = inTransaction();
        freeSymbolMapWriters();
        Misc.free(txWriter);
        Misc.free(metaMem);
        Misc.free(ddlMem);
        Misc.free(indexMem);
        Misc.free(other);
        Misc.free(todoMem);
        Misc.free(columnVersionWriter);
        freeColumns(truncate & !distressed);
        try {
            releaseLock(!truncate | tx | performRecovery | distressed);
        } finally {
            Misc.free(txnScoreboard);
            Misc.free(path);
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

        bumpStructureVersion();
        metadata.setTableVersion();
    }

    private void freeAndRemoveColumnPair(ObjList<MemoryMA> columns, int pi, int si) {
        Misc.free(columns.getAndSetQuick(pi, NullMemory.INSTANCE));
        Misc.free(columns.getAndSetQuick(si, NullMemory.INSTANCE));
    }

    private void freeColumns(boolean truncate) {
        // null check is because this method could be called from the constructor
        if (columns != null) {
            closeAppendMemoryTruncate(truncate);
        }
    }

    private void freeNullSetter(ObjList<Runnable> nullSetters, int columnIndex) {
        nullSetters.setQuick(columnIndex, NOOP);
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

    long getColumnTop(long partitionTimestamp, int columnIndex, long defaultValue) {
        // Check if there is explicit record for this partitionTimestamp / columnIndex combination
        int recordIndex = columnVersionWriter.getRecordIndex(partitionTimestamp, columnIndex);
        if (recordIndex > -1L) {
            return columnVersionWriter.getColumnTopByIndex(recordIndex);
        }

        // Check if column has been already added before this partition
        long columnTopDefaultPartition = columnVersionWriter.getColumnTopPartitionTimestamp(columnIndex);
        if (columnTopDefaultPartition <= partitionTimestamp) {
            return 0;
        }

        // This column does not exist in the partition
        return defaultValue;
    }

    long getColumnTop(int columnIndex) {
        return columnTops.getQuick(columnIndex);
    }

    CairoConfiguration getConfiguration() {
        return configuration;
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

    private MemoryMA getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private MemoryMA getPrimaryWalDColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return walDColumns.getQuick(getPrimaryColumnIndex(column));
    }

    private MemoryMA getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    private MemoryMA getSecondaryWalDColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return walDColumns.getQuick(getSecondaryColumnIndex(column));
    }

    MapWriter getSymbolMapWriter(int columnIndex) {
        return symbolMapWriters.getQuick(columnIndex);
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
                            LOG.info().$("indexing [path=").$(path).$(']').$();

                            createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);
                            final long partitionSize = txWriter.getPartitionSizeByPartitionTimestamp(timestamp);
                            final long columnTop = columnVersionWriter.getColumnTop(timestamp, columnIndex);

                            if (partitionSize > columnTop) {
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
                indexer.close();
            }
        }
    }

    private void indexLastPartition(SymbolColumnIndexer indexer, CharSequence columnName, long columnNameTxn, int columnIndex, int indexValueBlockSize) {
        final int plen = path.length();

        createIndexFiles(columnName, columnNameTxn, indexValueBlockSize, plen, true);

        final long lastPartitionTs = txWriter.getLastPartitionTimestamp();
        final long columnTop = columnVersionWriter.getColumnTop(lastPartitionTs, columnIndex);

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

    private void openColumnFiles(CharSequence name, long columnNameTxn, int columnIndex, int pathTrimToLen) {
        MemoryMA mem1 = getPrimaryColumn(columnIndex);
        MemoryMA mem2 = getSecondaryColumn(columnIndex);

        try {
            mem1.of(ff, dFile(
                    path.trimTo(pathTrimToLen), name, columnNameTxn),
                    configuration.getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WRITER,
                    configuration.getWriterFileOpenOpts()
            );
            if (mem2 != null) {
                mem2.of(
                        ff,
                        iFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                        configuration.getDataAppendPageSize(),
                        -1,
                        MemoryTag.MMAP_TABLE_WRITER,
                        configuration.getWriterFileOpenOpts()
                );
            }
        } finally {
            path.trimTo(pathTrimToLen);
        }
    }

    private void openWalDColumnFiles(CharSequence name, int columnIndex, int pathTrimToLen) {
        MemoryMA mem1 = getPrimaryWalDColumn(columnIndex);
        MemoryMA mem2 = getSecondaryWalDColumn(columnIndex);

        try {
            mem1.of(ff, walDFile(
                            walDPath.trimTo(pathTrimToLen), name),
                    configuration.getDataAppendPageSize(),
                    -1,
                    MemoryTag.MMAP_TABLE_WALD_WRITER,
                    configuration.getWriterFileOpenOpts()
            );
            if (mem2 != null) {
                mem2.of(
                        ff,
                        walDIFile(walDPath.trimTo(pathTrimToLen), name),
                        configuration.getDataAppendPageSize(),
                        -1,
                        MemoryTag.MMAP_TABLE_WALD_WRITER,
                        configuration.getWriterFileOpenOpts()
                );
            }
        } finally {
            walDPath.trimTo(pathTrimToLen);
        }
    }

    private void openFirstPartition(long timestamp) {
        final long ts = repairDataGaps(timestamp);
        openPartition(ts);
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

            long partitionTimestamp = txWriter.getPartitionTimestampLo(timestamp);
            for (int i = 0; i < columnCount; i++) {
                if (metadata.getColumnType(i) > 0) {
                    final CharSequence name = metadata.getColumnName(i);
                    long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, i);
                    final long columnTop;

                    openColumnFiles(name, columnNameTxn, i, plen);
                    columnTop = columnVersionWriter.getColumnTop(partitionTimestamp, i);
                    columnTops.extendAndSet(i, columnTop);
                }
            }
            LOG.info().$("switched partition [path='").$(path).$('\'').I$();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openNewWalDPartition() {
        waldPartitionCounter++;
        walDRowCounter = 0;
        try {
            walDPath.slash().put(waldPartitionCounter);
            int plen = walDPath.length();
            if (ff.mkdirs(walDPath.slash$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Cannot create WAL-D partition directory: ").put(walDPath);
            }
            walDPath.trimTo(plen);
            assert columnCount > 0;
            try (MemoryMAR metaMem = Vm.getMARInstance()) {
                openWalDMetaFile(ff, walDPath, plen, metaMem);
                copyMetadataTo(metaMem, false);
            }
            for (int i = 0; i < columnCount; i++) {
                final int type = metadata.getColumnType(i);
                if (type > 0) {
                    final CharSequence name = metadata.getColumnName(i);
                    openWalDColumnFiles(name, i, plen);
                    if (ColumnType.isSymbol(type)) {
                        try(MemoryMARW mem = Vm.getMARWInstance()) {
                            createSymbolMapFiles(
                                    ff,
                                    mem,
                                    walDPath.trimTo(walDRootLen),
                                    name,
                                    COLUMN_NAME_TXN_NONE,
                                    1024, // take this from TableStructure !!!
                                    false // take this from TableStructure !!!
                            );
                        }
                    }
                }
            }
            LOG.info().$("switched WAL-D partition [path='").$(walDPath).$('\'').I$();
        } catch (Throwable e) {
            distressed = true;
            throw e;
        } finally {
            walDPath.trimTo(walDRootLen);
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
        rollbackSymbolTables();
        performRecovery = false;
    }

    void purgeUnusedPartitions() {
        if (PartitionBy.isPartitioned(partitionBy)) {
            removeNonAttachedPartitions();
        }
    }

    private long readMinTimestamp(long partitionTimestamp) {
        setStateForTimestamp(other, partitionTimestamp, false);
        try {
            dFile(other, metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE);
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
        freeNullSetter(nullSetters, columnIndex);
        freeAndRemoveColumnPair(columns, pi, si);
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
                removeFileAndOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName, columnNameTxn));
                removeFileAndOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(rootLen), columnName, columnNameTxn));
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeColumnFilesInPartition(CharSequence columnName, int columnIndex, long partitionTimestamp, long partitionNameTxn) {
        setPathForPartition(path, partitionBy, partitionTimestamp, false);
        txnPartitionConditionally(path, partitionNameTxn);
        int plen = path.length();
        long columnNameTxn = columnVersionWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
        removeFileAndOrLog(ff, dFile(path, columnName, columnNameTxn));
        removeFileAndOrLog(ff, iFile(path.trimTo(plen), columnName, columnNameTxn));
        removeFileAndOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn));
        removeFileAndOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn));
        path.trimTo(rootLen);
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
            copyVersionAndLagValues(ddlMem, true);
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i, i == index, ddlMem);
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
        removeFileAndOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn));
        removeFileAndOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn));
        path.trimTo(rootLen);
    }

    private void removeLastColumn() {
        removeColumn(columnCount - 1);
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

    private void removeSymbolMapFilesQuiet(CharSequence name, long columnNamTxn) {
        try {
            removeFileAndOrLog(ff, offsetFileName(path.trimTo(rootLen), name, columnNamTxn));
            removeFileAndOrLog(ff, charFileName(path.trimTo(rootLen), name, columnNamTxn));
            removeFileAndOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(rootLen), name, columnNamTxn));
            removeFileAndOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(rootLen), name, columnNamTxn));
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
                renameFileOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(rootLen), columnName, columnNameTxn), BitmapIndexUtils.keyFileName(other.trimTo(rootLen), newName, columnNameTxn));
                renameFileOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(rootLen), columnName, columnNameTxn), BitmapIndexUtils.valueFileName(other.trimTo(rootLen), newName, columnNameTxn));
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
        renameFileOrLog(ff, BitmapIndexUtils.keyFileName(path.trimTo(plen), columnName, columnNameTxn), BitmapIndexUtils.keyFileName(other.trimTo(plen), newName, columnNameTxn));
        renameFileOrLog(ff, BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn), BitmapIndexUtils.valueFileName(other.trimTo(plen), newName, columnNameTxn));
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
            copyVersionAndLagValues(ddlMem, true);
            ddlMem.jumpTo(META_OFFSET_COLUMN_TYPES);

            for (int i = 0; i < columnCount; i++) {
                writeColumnEntry(i, false, ddlMem);
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
                            TableUtils.dFile(path.trimTo(p), metadata.getColumnName(metadata.getTimestampIndex()), COLUMN_NAME_TXN_NONE);
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

                txWriter.reset(fixedRowCount, transientRowCount, maxTimestamp, defaultCommitMode, denseSymbolMapWriters);
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
        txWriter.truncate(columnVersionWriter.getVersion());
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

    private void rollbackSymbolTables() {
        int expectedMapWriters = txWriter.unsafeReadSymbolColumnCount();
        for (int i = 0; i < expectedMapWriters; i++) {
            denseSymbolMapWriters.getQuick(i).rollback(txWriter.unsafeReadSymbolWriterIndexOffset(i));
        }
    }

     private void walDRowAppend(ObjList<Runnable> activeNullSetters) {
        for (int i = 0; i < columnCount; i++) {
            if (rowValueIsNotNull.getQuick(i) < masterRef) {
                activeNullSetters.getQuick(i).run();
            }
        }
        walDRowCounter++;
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

    void walDRowCancel() {
        long newMasterRef = masterRef - 1;
        walDRowValueIsNotNull.fill(0, columnCount, newMasterRef);
        openNewWalDPartition();
    }

    void rowCancel() {
        if ((masterRef & 1) == 0) {
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
    }

    private void setRowValueNotNull(int columnIndex) {
        assert rowValueIsNotNull.getQuick(columnIndex) != masterRef;
        rowValueIsNotNull.setQuick(columnIndex, masterRef);
    }

    private void setWalDRowValueNotNull(int columnIndex) {
        assert walDRowValueIsNotNull.getQuick(columnIndex) != masterRef;
        walDRowValueIsNotNull.setQuick(columnIndex, masterRef);
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
            final MemoryMA m2 = columns.getQuick(i * 2 + 1);
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

    private void validateSwapMeta(CharSequence columnName) {
        try {
            try {
                path.concat(META_SWAP_FILE_NAME);
                if (metaSwapIndex > 0) {
                    path.put('.').put(metaSwapIndex);
                }
                metaMem.smallFile(ff, path.$(), MemoryTag.MMAP_TABLE_WRITER);
                validationMap.clear();
                validate(metaMem, validationMap, ColumnType.VERSION);
            } finally {
                metaMem.close();
                path.trimTo(rootLen);
            }
        } catch (CairoException e) {
            runFragile(RECOVER_FROM_META_RENAME_FAILURE, columnName, e);
        }
    }

    private void writeColumnEntry(int i, boolean markDeleted, MemoryMAR targetDdlMem) {
        int columnType = getColumnType(metaMem, i);
        // When column is deleted it's written to metadata with negative type
        if (markDeleted) {
            columnType = -Math.abs(columnType);
        }
        targetDdlMem.putInt(columnType);

        long flags = 0;
        if (isColumnIndexed(metaMem, i)) {
            flags |= META_FLAG_BIT_INDEXED;
        }

        if (isSequential(metaMem, i)) {
            flags |= META_FLAG_BIT_SEQUENTIAL;
        }
        targetDdlMem.putLong(flags);
        targetDdlMem.putInt(getIndexBlockCapacity(metaMem, i));
        targetDdlMem.putLong(getColumnHash(metaMem, i));
        targetDdlMem.skip(8);
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

    private class RowImpl implements Row {
        @Override
        public void append() {
            rowAppend(activeNullSetters);
            walDRowAppend(activeNullSetters);
        }

        @Override
        public void cancel() {
            walDRowCancel();
            rowCancel();
        }

        @Override
        public void putBin(int columnIndex, long address, long len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(address, len));
            setRowValueNotNull(columnIndex);

            getSecondaryWalDColumn(columnIndex).putLong(getPrimaryWalDColumn(columnIndex).putBin(address, len));
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putBin(int columnIndex, BinarySequence sequence) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putBin(sequence));
            setRowValueNotNull(columnIndex);

            getSecondaryWalDColumn(columnIndex).putLong(getPrimaryWalDColumn(columnIndex).putBin(sequence));
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putBool(int columnIndex, boolean value) {
            getPrimaryColumn(columnIndex).putBool(value);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putBool(value);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putByte(int columnIndex, byte value) {
            getPrimaryColumn(columnIndex).putByte(value);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putByte(value);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putChar(int columnIndex, char value) {
            getPrimaryColumn(columnIndex).putChar(value);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putChar(value);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putDate(int columnIndex, long value) {
            putLong(columnIndex, value);
            // putLong calls setRowValueNotNull
        }

        @Override
        public void putDouble(int columnIndex, double value) {
            getPrimaryColumn(columnIndex).putDouble(value);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putDouble(value);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putFloat(int columnIndex, float value) {
            getPrimaryColumn(columnIndex).putFloat(value);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putFloat(value);
            setWalDRowValueNotNull(columnIndex);
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

            getPrimaryWalDColumn(columnIndex).putInt(value);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong(int columnIndex, long value) {
            getPrimaryColumn(columnIndex).putLong(value);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putLong(value);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, long l0, long l1, long l2, long l3) {
            getPrimaryColumn(columnIndex).putLong256(l0, l1, l2, l3);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putLong256(l1, l2, l2, l3);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, Long256 value) {
            getPrimaryColumn(columnIndex).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, CharSequence hexString) {
            getPrimaryColumn(columnIndex).putLong256(hexString);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putLong256(hexString);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end) {
            getPrimaryColumn(columnIndex).putLong256(hexString, start, end);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putLong256(hexString, start, end);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putShort(int columnIndex, short value) {
            getPrimaryColumn(columnIndex).putShort(value);
            setRowValueNotNull(columnIndex);

            getPrimaryWalDColumn(columnIndex).putShort(value);
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);

            getSecondaryWalDColumn(columnIndex).putLong(getPrimaryWalDColumn(columnIndex).putStr(value));
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, char value) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value));
            setRowValueNotNull(columnIndex);

            getSecondaryWalDColumn(columnIndex).putLong(getPrimaryWalDColumn(columnIndex).putStr(value));
            setWalDRowValueNotNull(columnIndex);
        }

        @Override
        public void putStr(int columnIndex, CharSequence value, int pos, int len) {
            getSecondaryColumn(columnIndex).putLong(getPrimaryColumn(columnIndex).putStr(value, pos, len));
            setRowValueNotNull(columnIndex);

            getSecondaryWalDColumn(columnIndex).putLong(getPrimaryWalDColumn(columnIndex).putStr(value, pos, len));
            setWalDRowValueNotNull(columnIndex);
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
            final MemoryA walDPrimaryColumn = getPrimaryWalDColumn(index);
            switch (ColumnType.tagOf(type)) {
                case ColumnType.GEOBYTE:
                    primaryColumn.putByte((byte) value);
                    walDPrimaryColumn.putByte((byte) value);
                    break;
                case ColumnType.GEOSHORT:
                    primaryColumn.putShort((short) value);
                    walDPrimaryColumn.putShort((short) value);
                    break;
                case ColumnType.GEOINT:
                    primaryColumn.putInt((int) value);
                    walDPrimaryColumn.putInt((int) value);
                    break;
                default:
                    primaryColumn.putLong(value);
                    walDPrimaryColumn.putLong(value);
                    break;
            }
            setRowValueNotNull(index);
            setWalDRowValueNotNull(index);
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
