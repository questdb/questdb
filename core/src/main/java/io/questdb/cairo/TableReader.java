/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.idx.IndexBwdNullReader;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.IndexFwdNullReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.MemoryCMRDetachedImpl;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.BitSet;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TableReader implements Closeable, SymbolTableSource {
    private static final Log LOG = LogFactory.getLog(TableReader.class);
    private static final int PARTITIONS_SLOT_OFFSET_SIZE = 1;
    private static final int PARTITIONS_SLOT_OFFSET_NAME_TXN = PARTITIONS_SLOT_OFFSET_SIZE + 1;
    private static final int PARTITIONS_SLOT_OFFSET_COLUMN_VERSION = PARTITIONS_SLOT_OFFSET_NAME_TXN + 1;
    private static final int PARTITIONS_SLOT_OFFSET_FORMAT = PARTITIONS_SLOT_OFFSET_COLUMN_VERSION + 1;
    private static final int PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN = PARTITIONS_SLOT_OFFSET_FORMAT + 1;
    // Plan 3 (composite partitioning) Task 6: was a reserved/padding slot; now carries the partition's
    // cellKey (0 for plain/dormant tables), mirroring TxReader#getPartitionCellKey(int). No stride
    // change -- PARTITIONS_SLOT_SIZE was already 8.
    private static final int PARTITIONS_SLOT_OFFSET_CELL_KEY = PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN + 1;
    private static final int PARTITIONS_SLOT_SIZE = 8; // must be power of 2
    private static final int PARTITIONS_SLOT_SIZE_MSB = Numbers.msb(PARTITIONS_SLOT_SIZE);
    private final BitSet activeColumns = new BitSet();
    private final MillisecondClock clock;
    private final ColumnVersionReader columnVersionReader;
    // Owning list for the read-side composite interners (dedicated dictionaries + _cell registry
    // SymbolMapReaders), opened in openSymbolMaps() and freed in freeSymbolMapReaders(). These have no
    // owning table column, so unlike symbolMapReaders they are never sized to columnCount / indexed by
    // column -- see compositeDicts (the dual-mode, dimension-indexed lookup facade over this list).
    private final ObjList<SymbolMapReader> compositeInternerReaders = new ObjList<>();
    private final CairoConfiguration configuration;
    private final int dbRootSize;
    private final FilesFacade ff;
    private final int id;
    private final int maxOpenPartitions;
    private final MessageBus messageBus;
    private final TableReaderMetadata metadata;
    private final ParquetMetaFileReader parquetMetaReader = new ParquetMetaFileReader();
    private final int partitionBy;
    private final PartitionOverwriteControl partitionOverwriteControl;
    private final Path path;
    private final int rootLen;
    private final ObjList<SymbolMapReader> symbolMapReaders = new ObjList<>();
    private final int timestampType;
    private final TxReader txFile;
    private final TxnScoreboard txnScoreboard;
    private int columnCount;
    private int columnCountShl;
    private LongList columnTops;
    private ObjList<MemoryCMR> columns;
    // Non-owning, dual-mode lookup facade over compositeInternerReaders; null for a plain/cluster-only
    // table (no composite interners). Never closed here -- see compositeInternerReaders for ownership.
    private CompositeDictionaries compositeDicts;
    // reused across keyOfDimensionValue() calls to avoid an allocation per TRUNCATE-dimension lookup
    private final StringSink compositeDimSink = new StringSink();
    private boolean hasActiveColumns;
    private ObjList<IndexReader> indexes;
    private int openPartitionCount;
    private LongList openPartitionInfo;
    private ObjList<ParquetPartitionDecoder> parquetMetaDecoders;
    private ObjList<MemoryCMR> parquetMetadataPartitions;
    private ObjList<MemoryCMR> parquetPartitions;
    private int partitionCount;
    private long rowCount;
    // Per-checkout scan profile -- controls kernel page-cache hints and
    // post-checkout partition retention. Reset to DEFAULT by goPassive() on
    // every pool return so cross-checkout leaks are impossible.
    private ReaderScanProfile scanProfile = ReaderScanProfile.DEFAULT;
    private TableToken tableToken;
    private long tempMem8b = Unsafe.malloc(8, MemoryTag.NATIVE_TABLE_READER);
    private long txColumnVersion;
    private long txPartitionVersion;
    private long txTruncateVersion;
    private long txn = TableUtils.INITIAL_TXN;
    private boolean txnAcquired = false;

    public TableReader(
            int id,
            CairoConfiguration configuration,
            @NotNull TableToken tableToken,
            TxnScoreboardPool scoreboardFactory) {
        this(id, configuration, tableToken, scoreboardFactory, null, null);
    }

    // Don't forget to change TableReader srcReader overload when changing this constructor.
    public TableReader(
            int id,
            CairoConfiguration configuration,
            @NotNull TableToken tableToken,
            TxnScoreboardPool scoreboardPool,
            @Nullable MessageBus messageBus,
            @Nullable PartitionOverwriteControl partitionOverwriteControl
    ) {
        this.id = id;
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        this.maxOpenPartitions = configuration.getInactiveReaderMaxOpenPartitions();
        this.ff = configuration.getFilesFacade();
        this.tableToken = tableToken;
        this.messageBus = messageBus;
        try {
            this.path = new Path();
            this.path.of(configuration.getDbRoot());
            this.dbRootSize = path.size();
            path.concat(tableToken.getDirName());
            this.rootLen = path.size();
            path.trimTo(rootLen);
            metadata = openMetaFile();
            timestampType = metadata.getTimestampType();
            partitionBy = metadata.getPartitionBy();
            columnVersionReader = new ColumnVersionReader().ofRO(ff, path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            txnScoreboard = scoreboardPool.getTxnScoreboard(tableToken);
            LOG.debug()
                    .$("open [id=").$(metadata.getTableId())
                    .$(", table=").$(tableToken)
                    .I$();
            // Plan 3b Task 2 investigated retiring this setComposite() call (Task 1's _txn marker makes
            // it redundant for any table with >= 1 committed partition -- see TxReader#unsafeLoadBaseOffset).
            // Reverted then: a composite table that had been CREATEd but never yet committed a single
            // partition had an on-disk marker still at 0 (upgrade-only -- TableUtils#createTxn wrote 0
            // unconditionally, and nothing had run finishABHeader with stride 8 yet), so a TableReader
            // opened in that window had no signal at all without this call and silently reported the
            // plain stride. Confirmed empirically: CompositeTxCellTest#testStrideDerivedFromComposite
            // opens getReader("c") on a just-CREATEd, zero-partition composite table and asserts
            // getLongsPerAttachedPartition() == 8; with this call removed it deterministically read back
            // 4 instead. See Plan 3b Task 2 report.
            // <p>
            // Plan 3b Task 3 closed that specific create-time window: createTxn now writes the real
            // marker (8 for composite) from CREATE, not just from the first commit onward, so the marker
            // alone would likely suffice here too now. Task 3 deliberately did not re-investigate
            // removing this call -- kept out of scope to keep that task's diff focused on the
            // marker-authoritative-from-creation fix -- so it remains, now agreeing with the marker in
            // every case rather than only for tables with >= 1 committed partition.
            txFile = new TxReader(ff);
            txFile.setComposite(metadata.getPartitionSpec().getDimensionCount() > 0);
            txFile.ofRO(
                    path.trimTo(rootLen).concat(TXN_FILE_NAME).$(),
                    timestampType,
                    partitionBy
            );
            path.trimTo(rootLen);
            reloadSlow(false);
            init();

            this.partitionOverwriteControl = partitionOverwriteControl;
            if (partitionOverwriteControl != null) {
                partitionOverwriteControl.acquirePartitions(this);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    // copyOf constructor.
    public TableReader(
            int id,
            CairoConfiguration configuration,
            TableReader srcReader,
            TxnScoreboardPool scoreboardPool,
            @Nullable MessageBus messageBus,
            @Nullable PartitionOverwriteControl partitionOverwriteControl
    ) {
        assert srcReader.isOpen() && srcReader.isActive();
        this.id = id;
        this.configuration = configuration;
        this.clock = configuration.getMillisecondClock();
        this.maxOpenPartitions = configuration.getInactiveReaderMaxOpenPartitions();
        this.ff = configuration.getFilesFacade();
        this.tableToken = srcReader.getTableToken();
        this.messageBus = messageBus;
        try {
            this.path = new Path();
            this.path.of(configuration.getDbRoot());
            this.dbRootSize = path.size();
            path.concat(tableToken.getDirName());
            this.rootLen = path.size();
            path.trimTo(rootLen);
            metadata = copyMeta(srcReader.metadata);
            timestampType = metadata.getTimestampType();
            partitionBy = metadata.getPartitionBy();
            columnVersionReader = new ColumnVersionReader().ofRO(ff, path.trimTo(rootLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
            txnScoreboard = scoreboardPool.getTxnScoreboard(tableToken);
            LOG.debug()
                    .$("open as copy [id=").$(metadata.getTableId())
                    .$(", table=").$(tableToken)
                    .$(", srcTxn=").$(srcReader.getTxn())
                    .I$();
            // Plan 3b Task 2/3: kept -- see the primary constructor's comment above its own txFile.ofRO()
            // a few dozen lines up (Task 2 reverted removing this call; Task 3 later made createTxn write
            // the real marker from CREATE, closing the specific create-time gap that revert was about, but
            // did not re-investigate removal here -- out of that task's scope).
            txFile = new TxReader(ff);
            txFile.setComposite(metadata.getPartitionSpec().getDimensionCount() > 0);
            txFile.ofRO(
                    path.trimTo(rootLen).concat(TXN_FILE_NAME).$(),
                    timestampType,
                    partitionBy
            );
            path.trimTo(rootLen);
            reloadAtTxn(srcReader, false);
            txPartitionVersion = txFile.getPartitionTableVersion();
            txColumnVersion = txFile.getColumnVersion();
            txTruncateVersion = txFile.getTruncateVersion();
            init();

            this.partitionOverwriteControl = partitionOverwriteControl;
            if (partitionOverwriteControl != null) {
                partitionOverwriteControl.acquirePartitions(this);
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    public static int getPrimaryColumnIndex(int base, int index) {
        return 2 + base + index * 2;
    }

    @TestOnly
    public int calculateOpenPartitionCount() {
        int openPartitionCount = 0;
        for (int partitionIndex = partitionCount - 1; partitionIndex > -1; partitionIndex--) {
            final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
            long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
            if (partitionSize > -1) {
                ++openPartitionCount;
            }
        }
        return openPartitionCount;
    }

    @Override
    public void close() {
        if (isOpen()) {
            goPassive();
            freeSymbolMapReaders();
            freeIndexCache();
            Misc.free(metadata);
            Misc.free(txFile);
            freeColumns();
            freeParquetPartitions();
            parquetMetaReader.clear();
            freeTempMem();
            Misc.free(txnScoreboard);
            Misc.free(path);
            Misc.free(columnVersionReader);
            LOG.debug().$("closed [table=").$(tableToken).I$();
        }
    }

    public void closeExcessPartitions() {
        // close all but N latest partitions
        int keepOpen = scanProfile == ReaderScanProfile.SEQUENTIAL_EVICT ? 0 : maxOpenPartitions;
        if (PartitionBy.isPartitioned(partitionBy) && openPartitionCount > keepOpen) {
            final int originallyOpen = openPartitionCount;
            int openCount = 0;
            for (int partitionIndex = partitionCount - 1; partitionIndex > -1; partitionIndex--) {
                final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionSize > -1 && ++openCount > keepOpen) {
                    closePartition(partitionIndex);
                    if (openCount == originallyOpen) {
                        // ok, we've closed enough
                        break;
                    }
                }
            }
        }
    }

    /**
     * Closes a specific partition, releasing its memory mappings.
     * This can be called when the caller is done reading a partition to free
     * page cache and reduce memory pressure.
     * <p>
     * The partition will be automatically re-opened if accessed again.
     *
     * @param partitionIndex the index of the partition to close
     */
    public void closePartitionByIndex(int partitionIndex) {
        if (partitionIndex < 0 || partitionIndex >= partitionCount) {
            return;
        }
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        if (partitionSize > -1) {
            closePartition(partitionIndex);
        }
    }

    public void dumpRawTxPartitionInfo(LongList container) {
        txFile.dumpRawTxPartitionInfo(container);
    }

    /**
     * Returns this reader's attached-partition record stride: {@link TableUtils#LONGS_PER_TX_ATTACHED_PARTITION}
     * (4, plain) or {@link TableUtils#LONGS_PER_TX_ATTACHED_PARTITION_COMPOSITE} (8, composite). Callers
     * that snapshot this reader's raw partitions via {@link #dumpRawTxPartitionInfo} (e.g. {@code
     * PartitionOverwriteControl}) must capture this stride at the same time -- the resulting flat
     * {@link LongList} carries no self-describing marker of its own once decoupled from the mapped
     * {@code _txn} memory.
     */
    public int getLongsPerAttachedPartition() {
        return txFile.getLongsPerAttachedPartition();
    }

    public long floorToPartitionTimestamp(long timestamp) {
        return txFile.getPartitionTimestampByTimestamp(timestamp);
    }

    /**
     * Returns a {@link ParquetPartitionDecoder} backed by the _pm sidecar file for the given
     * partition. The decoder reads metadata from the _pm file (no parquet footer parsing)
     * and delegates data decoding to the stateless Rust decode engine.
     *
     * @param partitionIndex the partition index
     * @return the initialized ParquetPartitionDecoder
     */
    public ParquetPartitionDecoder getAndInitParquetPartitionDecoder(int partitionIndex) {
        ParquetPartitionDecoder decoder = parquetMetaDecoders.getQuick(partitionIndex);
        if (decoder == null) {
            decoder = new ParquetPartitionDecoder();
            parquetMetaDecoders.setQuick(partitionIndex, decoder);
        }
        long parquetMetaAddr = getParquetMetadataAddr(partitionIndex);
        long parquetMetaSize = getParquetMetadataSize(partitionIndex);
        long parquetAddr = getParquetAddr(partitionIndex);
        long parquetSize = getParquetFileSize(partitionIndex);
        if (decoder.getParquetMetaAddr() != parquetMetaAddr || decoder.getParquetMetaSize() != parquetMetaSize) {
            decoder.of(parquetMetaAddr, parquetMetaSize, parquetAddr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
        }
        return decoder;
    }

    public MemoryCR getColumn(int absoluteIndex) {
        return columns.getQuick(absoluteIndex);
    }

    public int getColumnBase(int partitionIndex) {
        return partitionIndex << columnCountShl;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public long getColumnTop(int base, int columnIndex) {
        return columnTops.getQuick(base / 2 + columnIndex);
    }

    public ColumnVersionReader getColumnVersionReader() {
        return columnVersionReader;
    }

    /**
     * The read-side composite interners (dedicated dictionaries + {@code _cell} registry) for this
     * table, or {@code null} if the table has no composite interners (plain or cluster-only table).
     */
    public CompositeDictionaries getCompositeDictionaries() {
        return compositeDicts;
    }

    public long getDataVersion() {
        return txFile.getDataVersion();
    }

    public IndexReader getIndexReader(int partitionIndex, int columnIndex, int direction) {
        final int columnBase = getColumnBase(partitionIndex);
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        final long partitionTimestamp = txFile.getPartitionTimestampByIndex(partitionIndex);
        final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        final long partitionTxn = txFile.getPartitionNameTxn(partitionIndex);
        IndexReader indexReader = getIndexReaderIfExists(partitionIndex, columnIndex, direction);
        if (indexReader != null) {
            // Single choke point for refreshing the scoreboard pin on cached
            // readers. TableReader.txn advances through several paths
            // (goActive / reload / ...); setting it here covers all of them.
            indexReader.setPinnedTableTxn(txn);
            if (
                    !indexReader.isOpen()
                            || indexReader.getColumnTxn() != columnNameTxn
                            || indexReader.getPartitionTxn() != partitionTxn
            ) {
                int plen = path.size();
                try {
                    indexReader.of(
                            configuration,
                            pathGenNativePartition(partitionIndex, partitionTxn),
                            metadata.getColumnName(columnIndex),
                            columnNameTxn,
                            partitionTxn,
                            getColumnTop(columnBase, columnIndex),
                            metadata,
                            columnVersionReader,
                            partitionTimestamp
                    );
                } finally {
                    path.trimTo(plen);
                }
            } else {
                indexReader.reloadConditionally();
            }
            return indexReader;
        }
        return createIndexReaderAt(index, columnBase, columnIndex, columnNameTxn, direction, partitionTxn);
    }

    public IndexReader getIndexReaderIfExists(int partitionIndex, int columnIndex, int direction) {
        final int columnBase = getColumnBase(partitionIndex);
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        final int indexIndex = direction == IndexReader.DIR_BACKWARD ? index : index + 1;
        return indexes.getQuick(indexIndex);
    }

    public long getMaxTimestamp() {
        return txFile.getMaxTimestamp();
    }

    public int getMaxUncommittedRows() {
        return metadata.getMaxUncommittedRows();
    }

    public TableReaderMetadata getMetadata() {
        return metadata;
    }

    public long getMetadataVersion() {
        return txFile.getMetadataVersion();
    }

    public long getMinTimestamp() {
        return txFile.getMinTimestamp();
    }

    public long getO3MaxLag() {
        return metadata.getO3MaxLag();
    }

    public int getOpenPartitionCount() {
        return openPartitionCount;
    }

    /**
     * Returns previously open Parquet partition's mmapped address or 0 in case of a native partition.
     */
    public long getParquetAddr(int partitionIndex) {
        return parquetPartitions.getQuick(partitionIndex).addressOf(0);
    }

    /**
     * Returns previously open Parquet partition read size or -1 in case of a native partition.
     */
    public long getParquetFileSize(int partitionIndex) {
        return parquetPartitions.getQuick(partitionIndex).size();
    }

    public long getParquetMetadataAddr(int partitionIndex) {
        MemoryCMR mem = parquetMetadataPartitions.getQuick(partitionIndex);
        return mem != null && mem.isOpen() ? mem.addressOf(0) : 0;
    }

    public long getParquetMetadataSize(int partitionIndex) {
        MemoryCMR mem = parquetMetadataPartitions.getQuick(partitionIndex);
        return mem != null && mem.isOpen() ? mem.size() : 0;
    }

    /**
     * Plan 3 (composite partitioning) Task 6: returns the cellKey this reader recorded for the given
     * physical partition index (0 for a plain/dormant table), mirroring {@link TxReader#getPartitionCellKey(int)}.
     */
    public int getPartitionCellKey(int partitionIndex) {
        return (int) openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_CELL_KEY);
    }

    /**
     * Test-only: exposes the raw {@code PARTITIONS_SLOT_OFFSET_COLUMN_VERSION} slot (the value
     * {@code columnVersionReader.getMaxPartitionVersion(...)} resolved for this partition) so a test can
     * assert directly on the reader's own resolved state instead of independently recomputing it.
     */
    @TestOnly
    public long getPartitionColumnVersion(int partitionIndex) {
        return openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION);
    }

    /**
     * Test-only: exercises {@link #closeRewrittenPartitionFiles(int, int)} directly -- the same call
     * {@code reshuffleColumns}/{@code createNewColumnList} make, for each already-open partition, to
     * decide whether its currently-mapped files are still current before rebuilding the column list on
     * an ADD/DROP/RENAME COLUMN reload. Plan 3 Task 8 lock: this must resolve the partition's own
     * current nameTxn/size by its (ts, cellKey) identity, never by re-searching txFile for "the"
     * partition at this partition's bare timestamp (which, for a composite table with more than one
     * cell sharing that timestamp, silently returns cellKey 0's record instead of this partition's own).
     */
    @TestOnly
    public long testCloseRewrittenPartitionFiles(int partitionIndex) {
        return closeRewrittenPartitionFiles(partitionIndex, getColumnBase(partitionIndex));
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public byte getPartitionFormat(int partitionIndex) {
        return (byte) openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_FORMAT);
    }

    public byte getPartitionFormatFromMetadata(int partitionIndex) {
        if (txFile.isPartitionParquet(partitionIndex)) {
            return PartitionFormat.PARQUET;
        }
        return PartitionFormat.NATIVE;
    }

    @TestOnly
    public int getPartitionIndex(int columnBase) {
        return columnBase >>> columnCountShl;
    }

    public int getPartitionIndexByTimestamp(long timestamp) {
        int end = openPartitionInfo.binarySearchBlock(PARTITIONS_SLOT_SIZE_MSB, timestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (end < 0) {
            // This will return -1 if searched timestamp is before the first partition
            // The caller should handle negative return values
            return (-end - 2) / PARTITIONS_SLOT_SIZE;
        }
        return end / PARTITIONS_SLOT_SIZE;
    }

    /**
     * Same find-floor search as {@link #getPartitionIndexByTimestamp(long)}, but for an exact match
     * resolves to the HIGHEST partition index sharing that timestamp instead of the lowest -- i.e. a
     * composite table's LAST cell (highest cellKey) of the matched day, rather than its first (cellKey
     * 0). Used exclusively for interval-scan high-boundary resolution (see {@code
     * AbstractIntervalPartitionFrameCursor#cullPartitions}) so every sibling cell of the highest matched
     * day is included in {@code [partitionLo, partitionHi)}.
     * <p>
     * This is provably identical to {@link #getPartitionIndexByTimestamp(long)} in every case except an
     * exact match against a multi-entry (multi-cell) run:
     * <ul>
     *     <li>NOT-FOUND (timestamp strictly between two days, e.g. a gap day, or before/after every
     *     partition): {@code LongList#binarySearchBlock}'s scan-up and scan-down both fall through to
     *     the same linear {@code scanUpBlock}/{@code scanDownBlock} tail search, which normalizes to the
     *     identical insertion-point index regardless of direction -- there is no equal run to resolve
     *     differently.</li>
     *     <li>EXACT match, single-entry run (every day of a PLAIN table, since it has exactly one cell):
     *     {@code scrollUpBlock}/{@code scrollDownBlock} both degenerate to that same single index --
     *     there are no neighbouring equal entries for either direction to walk past.</li>
     * </ul>
     * Only an exact match against a multi-entry run (a composite table's multi-cell day) resolves
     * differently -- {@code scrollUpBlock} walks to the lowest index of the run, {@code scrollDownBlock}
     * to the highest. Do NOT alter {@link #getPartitionIndexByTimestamp(long)} itself -- it has other,
     * floor-search callers (e.g. the interval low boundary, which is already correct: cellKey 0 is the
     * lowest index of the low day, so starting there already includes that day's every sibling cell).
     */
    public int getPartitionIndexByTimestampScanDown(long timestamp) {
        int end = openPartitionInfo.binarySearchBlock(PARTITIONS_SLOT_SIZE_MSB, timestamp, Vect.BIN_SEARCH_SCAN_DOWN);
        if (end < 0) {
            // This will return -1 if searched timestamp is before the first partition
            // The caller should handle negative return values
            return (-end - 2) / PARTITIONS_SLOT_SIZE;
        }
        return end / PARTITIONS_SLOT_SIZE;
    }

    /**
     * Pretty convoluted logic to calculate upper timestamp bound of the given partition, e.g., by partition index.
     * First thing of note - the upper-bound value is inclusive, it must be a value that will be able to reside in the
     * partition.
     * <p>
     * The value of the upper bound is COMPUTED, rather than retrieved from a store. The inputs for the computation are:
     * - min timestamp of the partition - this is a stored value
     * - timestamp ceil function, which depends on the partition type. E.g., you could round any timestamp to the
     * theoretical upper bound. But we cannot use only this method because of partition splits.
     * - min timestamp of the next partition, e.g., if partition was split, we cannot use ceil function but rather
     * the min timestamp of the next partition MINUS ONE (to ensure timestamp is inclusive). However, we cannot use this
     * method only because of gaps in partitions. E.g., daily partition could have a gap for weekend.
     * - we also cannot just grab the next partition, and we need to be mindful of the partition count. To avoid
     * index-out-of-bounds errors.
     * <p>
     * To calculate the bound we will:
     * 1. check if we can access the next partition. If we cannot - it means this is the last partition, and we can
     * use the ceil function and that would be it.
     * 2. We can access the next partition - great, we get its min timestamp but, next gotcha - there could be a gap,
     * so we take a min between the ceil value and the next timestamp value.
     * <p>
     * Composite-partitioning gotcha (Task 5a-2): a composite table can attach MULTIPLE partition slots
     * sharing the exact same raw timestamp -- one per sibling CELL of the same logical (day/month/year)
     * partition, sorted (ts ASC, cellKey ASC). "The next partition" above is only guaranteed to start a
     * genuinely LATER day for the day's LAST cell; for any other (non-last) cell, the physically-next slot
     * is that SAME day's next cellKey, not a later day. We therefore skip past every such sibling (same
     * timestamp as this partition's own) before treating "the next partition" as the next-day candidate.
     * For a plain table (or an already-last cell of a day) this is a guaranteed no-op: no two entries ever
     * share a raw timestamp there, so the skip loop never executes and behaviour is byte-identical to
     * before this gotcha was handled. Mirrors {@link TxReader}'s own private {@code skipCompositeCellSiblings}
     * helper (used by {@link TxReader#getNextPartitionTimestamp} / {@link TxReader#getNextExistingPartitionTimestamp}),
     * which independently established the same "skip same-timestamp siblings" idiom for the write path.
     * <p>
     * Clear?
     *
     * @param partitionIndex the index of the partition in question
     * @return upper bound of the timestamp that can possibly be stored in this partition, which is an inclusive value.
     */
    public long getPartitionMaxTimestampFromMetadata(int partitionIndex) {
        final long ownTimestamp = getPartitionMinTimestampFromMetadata(partitionIndex);
        int next = partitionIndex + 1;
        while (next < getPartitionCount() && getPartitionMinTimestampFromMetadata(next) == ownTimestamp) {
            next++;
        }
        long minTimestampCeil = txFile.getNextLogicalPartitionTimestamp(ownTimestamp);
        return next < getPartitionCount() ? Math.min(getPartitionMinTimestampFromMetadata(next), minTimestampCeil) - 1 : minTimestampCeil;
    }

    public long getPartitionMinTimestampFromMetadata(int partitionIndex) {
        return txFile.getPartitionTimestampByIndex(partitionIndex);
    }

    public long getPartitionRowCount(int partitionIndex) {
        return openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
    }

    public long getPartitionRowCountFromMetadata(int partitionIndex) {
        return txFile.getPartitionSize(partitionIndex);
    }

    public long getPartitionTimestampByIndex(int partitionIndex) {
        return txFile.getPartitionTimestampByIndex(partitionIndex);
    }

    public int getPartitionedBy() {
        return metadata.getPartitionBy();
    }

    @TestOnly
    public ReaderScanProfile getScanProfile() {
        return scanProfile;
    }

    public long getSeqTxn() {
        return txFile.getSeqTxn();
    }

    public SymbolMapReader getSymbolMapReader(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex);
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex);
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public long getTransientRowCount() {
        return txFile.getTransientRowCount();
    }

    public TxReader getTxFile() {
        return txFile;
    }

    public long getTxn() {
        return txn;
    }

    public long getTxnMetadataVersion() {
        return txFile.getMetadataVersion();
    }

    public TxnScoreboard getTxnScoreboard() {
        return txnScoreboard;
    }

    public void goActive() {
        reload();
        if (partitionOverwriteControl != null) {
            partitionOverwriteControl.acquirePartitions(this);
        }
    }

    public void goActiveAtTxn(TableReader srcReader) {
        assert srcReader.isOpen() && srcReader.isActive();
        assert tableToken.equals(srcReader.getTableToken());

        // We may need to downgrade from newer txn to an older one.
        final boolean needsDowngrade = txn > srcReader.txn;
        if (needsDowngrade) {
            // Prepare for downgrade.
            if (partitionOverwriteControl != null) {
                // Mark partitions as unused before releasing txn in scoreboard
                // to avoid false positives in partition overwrite control
                partitionOverwriteControl.releasePartitions(this);
            }
            // close all latest partitions
            if (PartitionBy.isPartitioned(partitionBy)) {
                for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                    final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                    long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                    if (partitionSize > -1) {
                        closePartition(partitionIndex);
                    }
                }
            }
            freeSymbolMapReaders();
            freeIndexCache();
            freeColumns();
            freeParquetPartitions();
            // Remember to copy source metadata upfront - we don't need to deal with metadata transition index.
            metadata.loadFrom(srcReader.metadata);
        }
        // Copy source reader's state.
        reloadAtTxn(srcReader, true);
        if (needsDowngrade) {
            // We need to re-init txn versions and all lists.
            init();
        }
        // Reload partitions.
        reconcileOpenPartitions(txPartitionVersion, txColumnVersion, txTruncateVersion);
        // Save transaction details which impact the reloading.
        // Do not rely on txReader, it can be reloaded outside this method.
        txPartitionVersion = txFile.getPartitionTableVersion();
        txColumnVersion = txFile.getColumnVersion();
        txTruncateVersion = txFile.getTruncateVersion();

        if (partitionOverwriteControl != null) {
            partitionOverwriteControl.acquirePartitions(this);
        }
    }

    public void goPassive() {
        if (!isActive()) {
            return;
        }
        if (partitionOverwriteControl != null) {
            // Mark partitions as unused before releasing txn in scoreboard
            // to avoid false positives in partition overwrite control
            partitionOverwriteControl.releasePartitions(this);
        }
        if (releaseTxn() && PartitionBy.isPartitioned(partitionBy)) {
            // check if the reader unlocks a transaction in the scoreboard to
            // house-keep the partition versions
            checkSchedulePurgeO3Partitions();
        }
        closeExcessPartitions();
        hasActiveColumns = false;
        resetAllColumnsOpenFlag();
        scanProfile = ReaderScanProfile.DEFAULT;
    }

    public boolean hasParquetPartitions() {
        for (int i = 0; i < partitionCount; i++) {
            if (txFile.isPartitionParquet(i)) {
                return true;
            }
        }
        return false;
    }

    public boolean isActive() {
        return txnAcquired;
    }

    public boolean isColumnCached(int columnIndex) {
        return symbolMapReaders.getQuick(columnIndex).isCached();
    }

    public boolean isOpen() {
        return tempMem8b != 0;
    }

    /**
     * Maps a raw dimension value to its dense interned key on the read side -- the mirror of
     * {@link TableWriter#internDimensionValue(int, CharSequence)}, dispatching on the same
     * {@link PartitionDimension#getKind()}. Returns
     * {@link io.questdb.cairo.sql.SymbolTable#VALUE_NOT_FOUND} when {@code value} was never interned
     * ({@code IDENTITY}/{@code TRUNCATE} delegate to {@code keyOf}, which already returns it).
     * {@code EXPRESSION} dimensions are not supported here (Plan 4).
     */
    public int keyOfDimensionValue(int dimIndex, CharSequence value) {
        PartitionDimension dim = metadata.getPartitionSpec().getDimension(dimIndex);
        switch (dim.getKind()) {
            case PartitionDimension.KIND_IDENTITY:
                return getSymbolMapReader(denseIndexOfDimensionSource(dim)).keyOf(value);
            case PartitionDimension.KIND_HASH:
                return CompositeDimensionTransform.hashBucket(value, dim.getParam());
            case PartitionDimension.KIND_TRUNCATE:
                return getCompositeDictionaries().dictReaderFor(dimIndex).keyOf(
                        CompositeDimensionTransform.truncatedPrefix(value, dim.getParam(), compositeDimSink)
                );
            default:
                throw new UnsupportedOperationException("composite expression dimensions land in Plan 4");
        }
    }

    /**
     * Task 5b read-side counterpart of {@link #keyOfDimensionValue}: given a set of already-resolved
     * per-dimension ordinals for dimension {@code dimIndex} (each produced by {@link
     * #keyOfDimensionValue}, one call per predicate value), returns the set of cellKeys whose
     * registered dimension-tuple carries one of those ordinals at position {@code dimIndex} -- i.e.
     * every cell a {@code WHERE <dimension> = 'v'} / {@code IN (...)} predicate could possibly match.
     * Enumerates the full {@code _cell} registry (there is no reverse tuple-component index), mirroring
     * {@link #renderCellSegment}'s existing tuple-decode idiom.
     * <p>
     * An empty {@code allowedOrdinals} (every predicate value resolved to
     * {@link io.questdb.cairo.sql.SymbolTable#VALUE_NOT_FOUND}, i.e. never interned) correctly yields an
     * EMPTY result -- 0 matching cells, not "every cell" -- so a never-seen predicate value prunes to a
     * genuinely empty scan rather than falling back to "no pruning".
     * <p>
     * Caller ({@code SqlCodeGenerator}) is responsible for verifying {@code dimIndex} actually
     * corresponds to the predicate's own column and that every value function was safe to evaluate
     * right now (not a still-unbound runtime constant) before calling this; this method itself performs
     * no such validation, and does not need to -- it operates purely on already-resolved ordinals.
     */
    public IntHashSet resolveDimensionCellKeys(int dimIndex, IntHashSet allowedOrdinals) {
        final CellRegistry cellRegistry = getCompositeDictionaries().cellRegistry();
        final int[] tuple = new int[metadata.getPartitionSpec().getDimensionCount()];
        final IntHashSet allowedCellKeys = new IntHashSet();
        for (int ck = 0, n = cellRegistry.size(); ck < n; ck++) {
            cellRegistry.getTuple(ck, tuple);
            if (allowedOrdinals.contains(tuple[dimIndex])) {
                allowedCellKeys.add(ck);
            }
        }
        return allowedCellKeys;
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return getSymbolMapReader(columnIndex).newSymbolTableView();
    }

    /**
     * Opens given partition for reading. Native partitions become immediately readable
     * after this call through mapped memory. For Parquet partitions, the file is open
     * for read with fd available via {@link #getParquetAddr(int)}} call.
     *
     * @param partitionIndex partition index
     * @return partition size in rows
     */
    public long openPartition(int partitionIndex) {
        final long size = getPartitionRowCount(partitionIndex);
        if (size != -1) {
            final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
            if (openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN) == 0) {
                openMissingColumnsInPartition(partitionIndex, offset, size);
            }
            return size;
        }
        return openPartition0(partitionIndex);
    }

    public boolean reload() {
        if (acquireTxn()) {
            return false;
        }
        try {
            reloadSlow(true);
            // partition reload will apply truncate if necessary
            // applyTruncate for non-partitioned tables only
            reconcileOpenPartitions(txPartitionVersion, txColumnVersion, txTruncateVersion);

            // Save transaction details which impact the reloading.
            // Do not rely on txReader, it can be reloaded outside this method.
            txPartitionVersion = txFile.getPartitionTableVersion();
            txColumnVersion = txFile.getColumnVersion();
            txTruncateVersion = txFile.getTruncateVersion();

            // Useful for debugging
            // assert DebugUtils.reconcileColumnTops(PARTITIONS_SLOT_SIZE, openPartitionInfo, columnVersionReader, this);
            return true;
        } catch (Throwable e) {
            releaseTxn();
            throw e;
        }
    }

    public void setActiveColumns(@Nullable IntList columnIndexes) {
        resetAllColumnsOpenFlag();

        if (columnIndexes == null || columnIndexes.size() == 0) {
            hasActiveColumns = false;
            return;
        }
        activeColumns.clear();
        int distinctCount = 0;
        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            if (!activeColumns.getAndSet(columnIndexes.getQuick(i))) {
                distinctCount++;
            }
        }
        // When all columns are referenced, skip per-column BitSet checks
        // in openPartitionColumns().
        hasActiveColumns = distinctCount < columnCount;
    }

    /**
     * Sets the scan profile for the current checkout. See {@link ReaderScanProfile}
     * for the meaning of each value. Reset to {@link ReaderScanProfile#DEFAULT}
     * by {@link #goPassive()} on every pool return, so the profile is always
     * a per-checkout decision.
     *
     * @param profile the profile to adopt for the current checkout (non-null)
     */
    public void setScanProfile(ReaderScanProfile profile) {
        this.scanProfile = profile;
    }

    public long size() {
        return rowCount;
    }

    public void updateTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
        this.metadata.updateTableToken(tableToken);
    }

    /**
     * Reverse-looks-up dense interned key {@code key} for dimension {@code dimIndex} back to its
     * value -- the read-only half {@code TableWriter} has no counterpart for. {@code IDENTITY} and
     * {@code TRUNCATE} look the key up in their respective symbol map; {@code HASH} has no reverse (a
     * bucket cannot be un-hashed), so this returns {@code null}. {@code EXPRESSION} dimensions are
     * not supported here (Plan 4).
     */
    /**
     * Read-side counterpart of {@link TableWriter#renderCellSegment(CharSink, int)} (composite-
     * partitioning Plan 4a Task 4): renders this table's on-disk cell-directory segment for a
     * resolved {@code cellKey}, per {@link PartitionSpec#getNamingMode()} -- {@code MODE_HIVE} renders
     * each dimension as {@code <sourceColumnName>=<value>}, {@code MODE_PLAIN} the bare {@code <value>};
     * an arity-&gt;1 spec joins segments with {@code '/'}. Reuses {@link #valueOfDimensionKey(int, int)}
     * (already dispatches IDENTITY/TRUNCATE reverse-lookup correctly, {@code null} for HASH) rather than
     * re-deriving the per-kind dispatch the writer's version hand-rolls, since the reader-side reverse
     * lookup already exists as a public method for an unrelated caller.
     * <p>
     * Needed because {@link #formatNativePartitionDirName(int, Path, long)} -- confirmed the SOLE
     * native-partition-path construction site in this class -- previously called the plain (no cell
     * segment) {@link TableUtils#setPathForNativePartition(Path, int, int, long, long)} overload
     * unconditionally; every partition-open path in this class (openPartition0, reconcileOpenPartitions,
     * etc.) funnels through it, so a composite table's non-dormant (non-zero cellKey) partitions could
     * never actually be opened for reading before this fix -- confirmed directly (this exact gap is what
     * surfaced this method's necessity: an O3-routed composite commit's cell files went unread, "file does
     * not exist" against the bare day directory).
     *
     * @throws UnsupportedOperationException if called on a non-composite table
     */
    public void renderCellSegment(CharSink<?> sink, int cellKey) {
        PartitionSpec spec = metadata.getPartitionSpec();
        int dimCount = spec.getDimensionCount();
        if (dimCount <= 0) {
            throw new UnsupportedOperationException(
                    "renderCellSegment() must not be called on a non-composite table [table=" + tableToken + ']'
            );
        }
        int[] tuple = new int[dimCount];
        getCompositeDictionaries().cellRegistry().getTuple(cellKey, tuple);
        byte namingMode = spec.getNamingMode();
        for (int i = 0; i < dimCount; i++) {
            if (i > 0) {
                sink.put('/');
            }
            PartitionDimension dim = spec.getDimension(i);
            if (namingMode == PartitionSpec.MODE_HIVE) {
                // KIND_EXPRESSION has no source column (getColumnIndex() == -1 by construction --
                // composite-partitioning Plan 4e Task 2/3): use its alias instead, mirroring
                // TableWriter#renderDimensionSegment's identical MODE_HIVE prefix choice and how
                // SHOW CREATE TABLE already renders this dimension via its alias (see
                // PartitionDimension#toSink). Otherwise metadata.getColumnName(-1) below is an
                // uncontrolled ArrayIndexOutOfBoundsException -- this is the read-side twin of the
                // exact landmine Task 1 fixed on the write side.
                if (dim.getKind() == PartitionDimension.KIND_EXPRESSION) {
                    sink.put(dim.getAlias()).put('=');
                } else {
                    sink.put(metadata.getColumnName(dim.getColumnIndex())).put('=');
                }
            }
            if (dim.getKind() == PartitionDimension.KIND_HASH) {
                sink.put(tuple[i]);
            } else {
                TableUtils.putPathSafe(sink, valueOfDimensionKey(i, tuple[i]));
            }
        }
    }

    /**
     * Reverse-looks-up dense interned key {@code key} for dimension {@code dimIndex} back to its
     * value -- the read-only half {@code TableWriter} has no counterpart for. {@code IDENTITY} and
     * {@code TRUNCATE} look the key up in their respective symbol map; {@code HASH} has no reverse (a
     * bucket cannot be un-hashed), so this returns {@code null}. {@code EXPRESSION} (composite-
     * partitioning Plan 4e Task 2/3) is a pure dedicated-dict reverse lookup, byte-identical to
     * {@code TRUNCATE} -- NOT a re-evaluation of the expression (there is no {@code Function}-eval
     * bridge on the read side at all, nor does there need to be: the ordinal already IS the
     * dedicated dict's key, interned once at write/eval time -- see {@code
     * TableWriter#resolveExpressionDimensionOrdinal}/{@code internDimensionValue}). {@code
     * EXPRESSION} shares {@code TRUNCATE}'s dedicated-dict bucket ({@link CompositeInternerLayout}),
     * so {@link #getCompositeDictionaries()}'s reader-side {@code dictReaderFor(dimIndex)} is already
     * populated for it exactly as it is for a real {@code TRUNCATE} dimension -- no additional
     * provisioning needed here.
     */
    public CharSequence valueOfDimensionKey(int dimIndex, int key) {
        PartitionDimension dim = metadata.getPartitionSpec().getDimension(dimIndex);
        switch (dim.getKind()) {
            case PartitionDimension.KIND_IDENTITY:
                return getSymbolMapReader(denseIndexOfDimensionSource(dim)).valueOf(key);
            case PartitionDimension.KIND_TRUNCATE:
            case PartitionDimension.KIND_EXPRESSION:
                return getCompositeDictionaries().dictReaderFor(dimIndex).valueOf(key);
            case PartitionDimension.KIND_HASH:
                return null;
            default:
                throw new UnsupportedOperationException("unknown composite partition dimension kind: " + dim.getKind());
        }
    }

    /**
     * Resolves a composite dimension's stable WRITER index ({@link PartitionDimension#getColumnIndex()})
     * to the reader's current DENSE column index. {@code getSymbolMapReader}/{@code getColumnType} etc.
     * are all dense-indexed, but {@code TableReaderMetadata} compacts tombstoned columns out of its
     * dense list on reload ({@code readFromMem}/{@code applyTransition0} both skip {@code writerIndex < 0}
     * entries and assign dense position by insertion order), so writer index and dense index diverge
     * once a lower-writer-index column has been dropped (whole-branch review finding I2) -- unlike
     * {@code TableWriterMetadata}, which only tombstones in place and never renumbers, so the writer
     * side ({@link TableWriter#internDimensionValue}) needs no analogous translation. Mirrors the
     * linear-scan idiom already used for the same writer-to-dense translation in {@code
     * AbstractPostingIndexReader.denseIndexFromWriter} / {@code IndexBuilder}'s covering-column
     * resolution.
     * <p>
     * DDL guards reject dropping a dimension's own source column, so the "not found" branch should be
     * unreachable in practice; it is guarded defensively here rather than left to surface as a bare
     * AIOOBE out of {@code getSymbolMapReader}.
     */
    private int denseIndexOfDimensionSource(PartitionDimension dim) {
        int writerIndex = dim.getColumnIndex();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getWriterIndex(i) == writerIndex) {
                return i;
            }
        }
        throw CairoException.critical(0)
                .put("composite dimension source column not found [writerIndex=").put(writerIndex).put(']');
    }

    private static int getColumnBits(int columnCount) {
        return Math.max(Numbers.msb(Numbers.ceilPow2(columnCount) * 2), 0);
    }

    private static boolean growColumn(MemoryCMRDetachedImpl mem1, MemoryCMRDetachedImpl mem2, int columnType, long rowCount) {
        if (rowCount > 0) {
            if (ColumnType.isVarSize(columnType)) {
                if (mem2 == null) {
                    return false;
                }

                // Extend aux memory
                ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                long newSize = columnTypeDriver.getAuxVectorSize(rowCount);
                if (!mem2.tryChangeSize(newSize)) {
                    return false;
                }

                // Extend data memory
                long dataSize = columnTypeDriver.getDataVectorSizeAt(mem2.addressOf(0), rowCount - 1);
                if (mem1 != null) {
                    // because of dedup, the size of var data can grow or shrink
                    return mem1.tryChangeSize(dataSize);
                } else {
                    // dataSize can be 0 in case when it's a varchar column and all the values are inlined
                    // The data memory was not open, but now we need to open it. Mark the partition as not reloaded by returning false
                    return dataSize == 0;
                }
            } else {
                if (mem1 == null) {
                    return false;
                }

                return mem1.tryChangeSize(rowCount << ColumnType.pow2SizeOf(columnType));
            }
        }
        return true;
    }

    private boolean acquireTxn() {
        if (!txnAcquired) {
            try {
                if (txnScoreboard.acquireTxn(id, txn)) {
                    txnAcquired = true;
                } else {
                    return false;
                }
            } catch (CairoException ex) {
                // Scoreboard can be over allocated
                LOG.critical().$("cannot lock txn in scoreboard [table=").$(tableToken)
                        .$(", txn=").$(txn)
                        .$(", error=").$safe(ex.getFlyweightMessage())
                        .I$();
                throw ex;
            }
        }

        // txFile can also be reloaded in goPassive->checkSchedulePurgeO3Partitions
        // if txFile txn doesn't match reader txn, reader has to be slow reloaded
        if (txn == txFile.getTxn()) {
            // We have to be sure the last txn is acquired in Scoreboard
            // otherwise the writer can delete partition version files
            // between reading txn file and acquiring txn in the Scoreboard.
            Unsafe.loadFence();
            return txFile.getVersion() == txFile.unsafeReadVersion();
        }
        return false;
    }

    private void checkSchedulePurgeO3Partitions() {
        // In scoreboard V2, it is cheap to check that the txn released is not the max txn,
        // do it as a first step before more expensive checks.
        if (txnScoreboard.isOutdated(txn)) {
            long partitionTableVersion = txFile.getPartitionTableVersion();
            // In scoreboard V2 isTxnAvailable(txn) can be relatively expensive. We do this check at the end.
            if (txFile.unsafeLoadAll() && txFile.getPartitionTableVersion() > partitionTableVersion && txnScoreboard.isTxnAvailable(txn)) {
                // The last lock for this txn is released, and this is not the latest txn number
                // Schedule a job to clean up partition versions this reader may hold
                if (TableUtils.schedulePurgeO3Partitions(messageBus, tableToken, timestampType, partitionBy)) {
                    return;
                }

                LOG.error()
                        .$("could not queue purge partition task, queue is full [")
                        .$("table=").$(tableToken)
                        .$(", txn=").$(txn)
                        .$(']').$();
            }
        }
    }

    private void closeDeletedPartition(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        closePartitionResources(partitionIndex, offset);
        if (partitionSize > -1) {
            openPartitionCount--;
        }
        int columnBase = getColumnBase(partitionIndex);
        int baseIndex = getPrimaryColumnIndex(columnBase, 0);
        int newBaseIndex = getPrimaryColumnIndex(getColumnBase(partitionIndex + 1), 0);
        columns.remove(baseIndex, newBaseIndex - 1);
        indexes.remove(baseIndex, newBaseIndex - 1);

        int colTopStart = columnBase / 2;
        int columnSlotSize = getColumnBase(1);
        columnTops.removeIndexBlock(colTopStart, columnSlotSize / 2);

        Misc.free(parquetMetaDecoders.get(partitionIndex));
        Misc.free(parquetMetadataPartitions.get(partitionIndex));
        Misc.free(parquetPartitions.get(partitionIndex));
        parquetMetaDecoders.remove(partitionIndex);
        parquetMetadataPartitions.remove(partitionIndex);
        parquetPartitions.remove(partitionIndex);
        openPartitionInfo.removeIndexBlock(offset, PARTITIONS_SLOT_SIZE);
        LOG.info().$("closed deleted partition [table=").$(tableToken)
                .$(", ts=").$ts(ColumnType.getTimestampDriver(timestampType), partitionTimestamp)
                .$(", partitionIndex=").$(partitionIndex)
                .I$();
        partitionCount--;
    }

    private void closeIndexReader(int base, int columnIndex) {
        int index = getPrimaryColumnIndex(base, columnIndex);
        Misc.free(indexes.getQuick(index));
        Misc.free(indexes.getQuick(index + 1));
    }

    private void closeParquetPartition(int partitionIndex) {
        Misc.free(parquetMetaDecoders.getQuick(partitionIndex));
        parquetMetaDecoders.setQuick(partitionIndex, null);
        Misc.free(parquetMetadataPartitions.getQuick(partitionIndex));
        Misc.free(parquetPartitions.getQuick(partitionIndex));
        int columnBase = getColumnBase(partitionIndex);
        for (int i = 0; i < columnCount; i++) {
            closeIndexReader(columnBase, i);
        }
    }

    private void closePartition(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTimestamp = openPartitionInfo.getQuick(offset);
        long partitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
        closePartitionResources(partitionIndex, offset);
        LOG.info().$("closed partition [path=").$substr(dbRootSize, path)
                .$(", timestamp=").$ts(ColumnType.getTimestampDriver(timestampType), partitionTimestamp)
                .I$();
        if (partitionSize > -1) {
            openPartitionCount--;
        }
    }

    private void closePartitionColumn(int base, int columnIndex) {
        int index = getPrimaryColumnIndex(base, columnIndex);
        if (scanProfile != ReaderScanProfile.DEFAULT) {
            MemoryCMR mem = columns.get(index);
            if (mem != null) {
                ff.madvise(mem.addressOf(0), mem.size(), Files.POSIX_MADV_DONTNEED);
            }
            mem = columns.get(index + 1);
            if (mem != null) {
                ff.madvise(mem.addressOf(0), mem.size(), Files.POSIX_MADV_DONTNEED);
            }
        }
        Misc.free(columns.get(index));
        Misc.free(columns.get(index + 1));
        closeIndexReader(base, columnIndex);
    }

    private void closePartitionColumns(int columnBase) {
        for (int i = 0; i < columnCount; i++) {
            closePartitionColumn(columnBase, i);
        }
    }

    private void closePartitionResources(int partitionIndex, int offset) {
        // we will call this method even if partition has been closed already, or it doesn't exist,
        // hence we ignore the "unknown" format
        final byte format = getPartitionFormat(partitionIndex);
        switch (format) {
            case PartitionFormat.PARQUET:
                closeParquetPartition(partitionIndex);
                break;
            case PartitionFormat.NATIVE:
                int columnBase = getColumnBase(partitionIndex);
                closePartitionColumns(columnBase);
                // A partition that transitioned from PARQUET to NATIVE still has
                // parquet resources (data.parquet mmap, _pm mmap, decoder) that
                // must be released. The format slot is updated before this method
                // runs, so we cannot rely on it to decide cleanup.
                closeParquetPartition(partitionIndex);
                break;
            default:
                break;
        }
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 0);
    }

    private long closeRewrittenPartitionFiles(int partitionIndex, int oldBase) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        long partitionTs = openPartitionInfo.getQuick(offset);
        long existingPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
        // Plan 3 Task 8: re-locate this partition by its stable (ts, cellKey) identity, not a bare
        // timestamp. This method's only callers (reshuffleColumns/createNewColumnList) run inside
        // reloadSlow, BEFORE reconcileOpenPartitions has resynced partitionCount/openPartitionInfo to
        // the just-loaded txFile -- so `partitionIndex` cannot be trusted as a raw txFile offset (an
        // earlier sibling partition insert/delete may already have shifted it there); a plain
        // by-timestamp scan is shift-safe but would additionally collapse onto cellKey 0's record
        // whenever more than one cell shares partitionTs. (ts, cellKey) is the same stable total-order
        // key reconcileOpenPartitions0 merges on, so this is both shift-safe and cell-safe.
        final int cellKey = (int) openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_CELL_KEY);
        final int rawIndex = txFile.findAttachedPartitionRawIndexBy(partitionTs, cellKey);
        long newNameTxn = rawIndex > -1 ? txFile.getPartitionNameTxnByRawIndex(rawIndex) : -1;
        long newSize = rawIndex > -1 ? txFile.getPartitionSizeByRawIndex(rawIndex) : -1;
        if (existingPartitionNameTxn != newNameTxn || newSize < 0) {
            LOG.debug().$("close outdated partition files [table=").$(tableToken).$(", ts=")
                    .$ts(ColumnType.getTimestampDriver(timestampType), partitionTs).$(", nameTxn=").$(newNameTxn).$();
            // Close all columns, partition is overwritten. Partition reconciliation process will re-open correct files
            if (getPartitionFormat(partitionIndex) == PartitionFormat.NATIVE) {
                for (int i = 0; i < columnCount; i++) {
                    closePartitionColumn(oldBase, i);
                }
            } else if (getPartitionFormat(partitionIndex) == PartitionFormat.PARQUET) {
                closeParquetPartition(partitionIndex);
            }
            openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
            openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 0);
            openPartitionCount--;
            return -1;
        }
        long nameTxn = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_NAME_TXN);
        //noinspection resource
        pathGenNativePartition(partitionIndex, nameTxn);
        return newSize;
    }

    private void copyColumns(
            int fromBase,
            int fromColumnIndex,
            ObjList<MemoryCMR> toColumns,
            LongList toColumnTops,
            ObjList<IndexReader> toIndexReaders,
            int toBase,
            int toColumnIndex
    ) {
        final int fromIndex = getPrimaryColumnIndex(fromBase, fromColumnIndex);
        final int toIndex = getPrimaryColumnIndex(toBase, toColumnIndex);

        toColumns.setQuick(toIndex, columns.getAndSetQuick(fromIndex, null));
        toColumns.setQuick(toIndex + 1, columns.getAndSetQuick(fromIndex + 1, null));
        toColumnTops.setQuick(toBase / 2 + toColumnIndex, columnTops.getQuick(fromBase / 2 + fromColumnIndex));
        toIndexReaders.setQuick(toIndex, indexes.getAndSetQuick(fromIndex, null));
        toIndexReaders.setQuick(toIndex + 1, indexes.getAndSetQuick(fromIndex + 1, null));
    }

    private TableReaderMetadata copyMeta(TableReaderMetadata srcMeta) {
        TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken);
        try {
            metadata.loadFrom(srcMeta);
            return metadata;
        } catch (Throwable th) {
            metadata.close();
            throw th;
        }
    }

    private IndexReader createIndexReaderAt(int globalIndex, int columnBase, int columnIndex, long columnNameTxn, int direction, long partitionTxn) {
        IndexReader reader;
        if (!metadata.isColumnIndexed(columnIndex)) {
            throw CairoException.critical(0).put("Not indexed: ").put(metadata.getColumnName(columnIndex));
        }
        MemoryR col = columns.getQuick(globalIndex);
        if (col instanceof NullMemoryCMR) {
            if (direction == IndexReader.DIR_BACKWARD) {
                reader = new IndexBwdNullReader(columnNameTxn, partitionTxn);
                indexes.setQuick(globalIndex, reader);
            } else {
                reader = new IndexFwdNullReader(columnNameTxn, partitionTxn);
                indexes.setQuick(globalIndex + 1, reader);
            }
        } else {
            int partitionIndex = getPartitionIndex(columnBase);
            Path path = pathGenNativePartition(partitionIndex, partitionTxn);
            try {
                final byte indexType = metadata.getColumnIndexType(columnIndex);
                final long partitionTimestamp = txFile.getPartitionTimestampByIndex(partitionIndex);
                reader = IndexFactory.createReader(
                        indexType,
                        direction,
                        configuration,
                        path,
                        metadata.getColumnName(columnIndex),
                        columnNameTxn,
                        partitionTxn,
                        getColumnTop(columnBase, columnIndex),
                        metadata,
                        columnVersionReader,
                        partitionTimestamp,
                        txn
                );
                if (direction == IndexReader.DIR_BACKWARD) {
                    indexes.setQuick(globalIndex, reader);
                } else {
                    indexes.setQuick(globalIndex + 1, reader);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
        return reader;
    }

    private void createNewColumnList(int columnCount, TableReaderMetadataTransitionIndex transitionIndex, int columnCountShl) {
        LOG.debug().$("resizing columns file list [table=").$(tableToken).I$();
        int capacity = partitionCount << columnCountShl;
        final ObjList<MemoryCMR> toColumns = new ObjList<>(capacity + 2);
        final LongList toColumnTops = new LongList(capacity / 2);
        final ObjList<IndexReader> toIndexReaders = new ObjList<>(capacity);
        toColumns.setPos(capacity + 2);
        toColumns.setQuick(0, NullMemoryCMR.INSTANCE);
        toColumns.setQuick(1, NullMemoryCMR.INSTANCE);
        toColumnTops.setPos(capacity / 2);
        toIndexReaders.setPos(capacity + 2);
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            final int toBase = partitionIndex << columnCountShl;
            final int fromBase = partitionIndex << this.columnCountShl;

            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1 && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, fromBase)) > -1) {
                    for (int i = 0; i < iterateCount; i++) {
                        if (transitionIndex.closeColumn(i)) {
                            closePartitionColumn(fromBase, i);
                        }

                        if (transitionIndex.replaceWithNew(i)) {
                            // new instance
                            reloadColumnAt(partitionIndex, path, toColumns, toColumnTops, toIndexReaders, toBase, i, partitionRowCount);
                        } else {
                            final int fromColumnIndex = transitionIndex.getCopyFromIndex(i);
                            assert fromColumnIndex < this.columnCount;
                            copyColumns(fromBase, fromColumnIndex, toColumns, toColumnTops, toIndexReaders, toBase, i);
                        }
                    }
                }
            } catch (Throwable th) {
                closePartitionColumns(fromBase);
                openPartitionInfo.setQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, -1);
                Misc.freeObjListIfCloseable(toColumns);
                throw th;
            } finally {
                path.trimTo(rootLen);
            }
        }
        this.columns = toColumns;
        this.columnTops = toColumnTops;
        this.columnCountShl = columnCountShl;
        this.indexes = toIndexReaders;
    }

    /**
     * Composite-partitioning (Plan 4a Task 4): resolves partition {@code partitionIndex}'s cell
     * segment for path construction, or {@code null} if this partition is DORMANT -- written before
     * Task 4's real per-row routing ever ran for this table (e.g. via the direct {@code newRow}/
     * {@code switchPartition} commit path, which never calls {@code resolveCellKey}/{@code
     * CellRegistry.internCell} at all; also every pre-Task-4-created composite table, such as every
     * {@code CompositeEndToEndTest}/{@code CompositePartitionDdlTest} fixture -- composite tables
     * with real rows but an EMPTY {@code _cell} registry, confirmed directly, not hypothetical).
     * {@code _txn}'s cellKey slot defaults to 0 as a bare structural value in that case (Plan 3:
     * "real writes only ever produce cellKey 0 today"), NOT as a genuinely-interned ordinal --
     * reverse-looking it up via {@link #renderCellSegment(CharSink, int)} would either throw
     * (reading past an empty/short symbol map) or return nonsense. A cellKey is only a safe
     * reverse-lookup target once the registry has actually interned that many entries ({@code
     * internCell} assigns dense ordinals {@code [0, size)} in intern order) -- otherwise this
     * partition predates real routing and must keep the exact pre-Task-4 bare-directory layout.
     *
     * @param scratchSink reused as the render target when non-dormant; caller-provided so both call
     *                    sites (native partition path, error-message path) can pick their own
     *                    allocation lifetime/sharing policy
     */
    private CharSequence resolveCellSegmentOrNullIfDormant(int partitionIndex, StringSink scratchSink) {
        if (metadata.getPartitionSpec().getDimensionCount() <= 0) {
            return null;
        }
        int cellKey = getPartitionCellKey(partitionIndex);
        if (cellKey >= getCompositeDictionaries().cellRegistry().size()) {
            return null;
        }
        renderCellSegment(scratchSink, cellKey);
        return scratchSink;
    }

    private void formatErrorPartitionDirName(int partitionIndex, Utf16Sink sink) {
        // Composite-partitioning (Plan 4a Task 4): mirrors formatNativePartitionDirName's own
        // cellSegment rendering -- see that method's and resolveCellSegmentOrNullIfDormant's own
        // docs. Error-message-only, not a write/read hot path, so a fresh scratch sink (not the
        // shared thread-local one, to avoid clobbering whatever the CALLER might itself be
        // assembling into a thread-local sink) is fine here.
        TableUtils.setSinkForNativePartition(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                -1,
                resolveCellSegmentOrNullIfDormant(partitionIndex, new StringSink())
        );
    }

    private void formatNativePartitionDirName(int partitionIndex, Path sink, long nameTxn) {
        // Composite-partitioning (Plan 4a Task 4): render this partition's own cell segment (null for
        // a plain/dormant table -- byte-identical to the pre-Task-4 behavior this replaces). Every
        // native-partition path construction in this class funnels through this one method.
        // A FRESH StringSink (not the shared Misc.getThreadLocalSink() instance) is deliberate, not
        // an over-caution: renderCellSegment -> valueOfDimensionKey -> SymbolMapReader.valueOf(key)
        // (IDENTITY reverse lookup) internally decodes through that SAME shared thread-local sink,
        // so using it as this method's own accumulator too is genuinely reentrant -- confirmed by
        // reproducing it directly (the shared sink's own decode of the FIRST dimension's value
        // clobbered/prefixed what this method had already accumulated for a later dimension/column-
        // name write, corrupting the rendered segment). Partition-open is not a per-row hot path, so
        // a small fresh allocation per call is the right trade-off here, unlike the writer-side
        // per-cell-dispatch sink (cellSegmentSink in dispatchCompositeCellRange), which reuses one
        // instance across many dispatches within a call but is never handed to a reverse-lookup that
        // itself reenters the same shared sink.
        TableUtils.setPathForNativePartition(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                nameTxn,
                resolveCellSegmentOrNullIfDormant(partitionIndex, new StringSink())
        );
    }

    private void formatParquetPartitionFileName(int partitionIndex, Path sink, long nameTxn) {
        TableUtils.setPathForParquetPartition(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                nameTxn
        );
    }

    private void formatParquetPartitionMetadataFileName(int partitionIndex, Path sink, long nameTxn) {
        TableUtils.setPathForParquetPartitionMetadata(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                nameTxn
        );
    }

    private void freeColumns() {
        Misc.freeObjList(columns);
    }

    private void freeIndexCache() {
        Misc.freeObjList(indexes);
    }

    private void freeParquetPartitions() {
        Misc.freeObjList(parquetMetaDecoders);
        Misc.freeObjList(parquetMetadataPartitions);
        Misc.freeObjList(parquetPartitions);
    }

    private void freeSymbolMapReaders() {
        for (int i = 0, n = symbolMapReaders.size(); i < n; i++) {
            Misc.freeIfCloseable(symbolMapReaders.getQuick(i));
        }
        symbolMapReaders.clear();
        // Non-owning: just drop the holder. Its dedicated-dict-reader and registry-reader
        // SymbolMapReaders are entries in compositeInternerReaders and are freed by the loop below
        // (freeing here too would double-free).
        compositeDicts = null;
        for (int i = 0, n = compositeInternerReaders.size(); i < n; i++) {
            Misc.freeIfCloseable(compositeInternerReaders.getQuick(i));
        }
        compositeInternerReaders.clear();
    }

    private void freeTempMem() {
        if (tempMem8b != 0) {
            tempMem8b = Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
        }
    }

    private void init() {
        txPartitionVersion = txFile.getPartitionTableVersion();
        txColumnVersion = txFile.getColumnVersion();
        txTruncateVersion = txFile.getTruncateVersion();

        columnCount = metadata.getColumnCount();
        columnCountShl = getColumnBits(columnCount);
        openSymbolMaps();
        partitionCount = txFile.getPartitionCount();

        int capacity = getColumnBase(partitionCount);
        parquetMetadataPartitions = new ObjList<>(partitionCount);
        parquetMetadataPartitions.setAll(partitionCount, NullMemoryCMR.INSTANCE);
        parquetPartitions = new ObjList<>(partitionCount);
        parquetPartitions.setAll(partitionCount, NullMemoryCMR.INSTANCE);
        parquetMetaDecoders = new ObjList<>(partitionCount);
        parquetMetaDecoders.setAll(partitionCount, null);
        columns = new ObjList<>(capacity + 2);
        columns.setPos(capacity + 2);
        columns.setQuick(0, NullMemoryCMR.INSTANCE);
        columns.setQuick(1, NullMemoryCMR.INSTANCE);
        indexes = new ObjList<>(capacity + 2);
        indexes.setPos(capacity + 2);

        openPartitionInfo = initOpenPartitionInfo();
        columnTops = new LongList(capacity / 2);
        columnTops.setPos(capacity / 2);
    }

    private @NotNull LongList initOpenPartitionInfo() {
        final LongList openPartitionInfo = new LongList(partitionCount * PARTITIONS_SLOT_SIZE);
        openPartitionInfo.setPos(partitionCount * PARTITIONS_SLOT_SIZE);
        for (int i = 0; i < partitionCount; i++) {
            // ts, number of rows, txn, column version for each partition
            // it is compared to attachedPartitions within the txn file to determine if a partition needs to be reloaded or not
            final int baseOffset = i * PARTITIONS_SLOT_SIZE;
            final long partitionTimestamp = txFile.getPartitionTimestampByIndex(i);
            final boolean isParquet = txFile.isPartitionParquet(i);
            final int cellKey = txFile.getPartitionCellKey(i);
            openPartitionInfo.setQuick(baseOffset, partitionTimestamp);
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_SIZE, -1); // -1 means it is not open
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_NAME_TXN, txFile.getPartitionNameTxn(i));
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp, cellKey));
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_FORMAT, isParquet ? PartitionFormat.PARQUET : PartitionFormat.NATIVE);
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 0);
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_CELL_KEY, cellKey);
        }
        return openPartitionInfo;
    }

    /**
     * Plan 3 Task 7 (T7-a): {@code cellKey} MUST be the inserted partition's own cellKey (0 for
     * plain/dormant-composite tables), sourced by the caller from {@code txFile.getPartitionCellKey(...)}
     * for the tx-side partition this insert represents. {@code LongList.insert} arraycopy-shifts the
     * array up WITHOUT zeroing the newly-opened region -- {@code openPartitionInfo.insert} below reveals
     * slot 6 still holding whatever a sibling record's bytes left there -- so every slot the fresh-open
     * {@code initOpenPartitionInfo} sets must be set here too, cellKey included, or the inserted
     * partition silently reads back a stale/wrong cellKey instead of its own.
     */
    private void insertPartition(int partitionIndex, long timestamp, int cellKey) {
        final int columnBase = getColumnBase(partitionIndex);
        final int columnSlotSize = getColumnBase(1);

        final int idx = getPrimaryColumnIndex(columnBase, 0);
        columns.insert(idx, columnSlotSize, NullMemoryCMR.INSTANCE);
        indexes.insert(idx, columnSlotSize, null);
        parquetMetadataPartitions.insert(partitionIndex, 1, NullMemoryCMR.INSTANCE);
        parquetPartitions.insert(partitionIndex, 1, NullMemoryCMR.INSTANCE);
        parquetMetaDecoders.insert(partitionIndex, 1, null);

        final int topBase = columnBase / 2;
        final int topSlotSize = columnSlotSize / 2;
        columnTops.insert(topBase, topSlotSize);
        columnTops.seed(topBase, topSlotSize, 0);

        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        openPartitionInfo.insert(offset, PARTITIONS_SLOT_SIZE);
        openPartitionInfo.setQuick(offset, timestamp);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT, -1);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 0);
        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_CELL_KEY, cellKey);
        partitionCount++;
        LOG.debug().$("inserted partition [index=").$(partitionIndex).$(", table=").$(tableToken)
                .$(", timestamp=").$ts(ColumnType.getTimestampDriver(timestampType), timestamp).I$();
    }

    // this method is not thread safe
    @NotNull
    private SymbolMapReaderImpl newSymbolMapReader(int symbolColumnIndex, int columnIndex) {
        // symbol column index is the index of symbol column in a dense array of symbol columns, e.g.,
        // if table has only one symbol columns, the symbolColumnIndex is 0 regardless of column position
        // in the metadata.
        return new SymbolMapReaderImpl(
                configuration,
                path,
                metadata.getColumnName(columnIndex),
                columnVersionReader.getSymbolTableNameTxn(metadata.getWriterIndex(columnIndex)),
                txFile.getSymbolValueCount(symbolColumnIndex)
        );
    }

    private TableReaderMetadata openMetaFile() {
        TableReaderMetadata metadata = new TableReaderMetadata(configuration, tableToken);
        try {
            metadata.loadMetadata();
            return metadata;
        } catch (Throwable th) {
            metadata.close();
            throw th;
        }
    }

    private void openMissingColumnsInPartition(int partitionIndex, int offset, long partitionSize) {
        final int columnBase = getColumnBase(partitionIndex);
        boolean hasNewColumns = false;
        try {
            for (int i = 0; i < columnCount; i++) {
                if (hasActiveColumns && !activeColumns.get(i)) {
                    continue;
                }
                final int primaryIndex = getPrimaryColumnIndex(columnBase, i);
                // For var-size columns the primary (data) pageAddress can be 0 when
                // all values are inlined in the aux vector, so check the aux column.
                final int checkIndex = ColumnType.isVarSize(metadata.getColumnType(i)) ? primaryIndex + 1 : primaryIndex;
                final MemoryCMR mem = columns.getQuick(checkIndex);
                if (mem != null && mem != NullMemoryCMR.INSTANCE && mem.isOpen()) {
                    continue; // already mapped
                }
                if (!hasNewColumns) {
                    final long nameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                    //noinspection resource
                    pathGenNativePartition(partitionIndex, nameTxn);
                    hasNewColumns = true;
                }
                reloadColumnAt(
                        partitionIndex,
                        path,
                        columns,
                        columnTops,
                        indexes,
                        columnBase,
                        i,
                        partitionSize
                );
            }
        } catch (Throwable th) {
            closePartitionColumns(columnBase);
            openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, -1);
            openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 0);
            openPartitionCount--;
            throw th;
        } finally {
            path.trimTo(rootLen);
        }

        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 1);
    }

    @NotNull
    private MemoryCMRDetachedImpl openOrCreateColumnMemory(
            Path path,
            ObjList<MemoryCMR> columns,
            int primaryIndex,
            @Nullable MemoryCMR mem,
            long columnSize,
            boolean keepFdOpen
    ) {
        // Sequential scan profiles hint the kernel to read ahead and to
        // release page cache after reading, avoiding memory pressure during
        // large scans. closePartitionColumn() applies the matching DONTNEED
        // hint at unmap time.
        final int madviseOpts = scanProfile != ReaderScanProfile.DEFAULT ? Files.POSIX_MADV_SEQUENTIAL : -1;
        MemoryCMRDetachedImpl memory;
        if (mem != null && mem != NullMemoryCMR.INSTANCE) {
            memory = (MemoryCMRDetachedImpl) mem;
            memory.of(ff, path.$(), columnSize, columnSize, MemoryTag.MMAP_TABLE_READER, 0, madviseOpts, keepFdOpen);
        } else {
            memory = new MemoryCMRDetachedImpl(ff, path.$(), columnSize, MemoryTag.MMAP_TABLE_READER, keepFdOpen, madviseOpts);
            columns.setQuick(primaryIndex, memory);
        }
        return memory;
    }

    /**
     * Opens (or remaps) the _pm metadata file for the given partition and
     * returns the parquet file size derived from its footer metadata.
     */
    private long openParquetMetadata(int partitionIndex, long partitionNameTxn) {
        final long parquetFileSize = txFile.getPartitionParquetFileSize(partitionIndex);
        assert parquetFileSize > 0;

        path.trimTo(rootLen);
        pathGenParquetPartitionMetadata(partitionIndex, partitionNameTxn);

        MemoryCMRDetachedImpl parquetMetaMem;
        final MemoryCMR existing = parquetMetadataPartitions.getQuick(partitionIndex);
        if (existing != null && existing != NullMemoryCMR.INSTANCE) {
            parquetMetaMem = (MemoryCMRDetachedImpl) existing;
        } else {
            parquetMetaMem = new MemoryCMRDetachedImpl();
            parquetMetadataPartitions.setQuick(partitionIndex, parquetMetaMem);
        }
        parquetMetaMem.ofWithSizeFromHeader(ff, path.$(), MemoryTag.MMAP_PARQUET_METADATA_READER);

        parquetMetaReader.of(parquetMetaMem.addressOf(0), parquetMetaMem.size());
        if (!parquetMetaReader.resolveFooter(parquetFileSize)) {
            throw CairoException.critical(0).put("invalid _pm file: failed to resolve footer [path=").put(path).put(']');
        }
        return parquetMetaReader.getParquetFileSize();
    }

    private long openPartition0(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        if (txFile.getPartitionCount() < 2 && txFile.getTransientRowCount() == 0) {
            return -1;
        }

        try {
            path.trimTo(rootLen);
            final long partitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
            // Plan 3 Task 6: source this partition's own cellKey (0 for plain/dormant) so the
            // column-version resolved below doesn't alias another cell sharing this timestamp.
            final int cellKey = txFile.getPartitionCellKey(partitionIndex);

            if (txFile.isPartitionParquet(partitionIndex)) {
                Path path = pathGenParquetPartition(partitionIndex, partitionNameTxn);
                if (ff.exists(path.$())) {
                    final long partitionSize = getPartitionRowCountFromMetadata(partitionIndex);
                    if (partitionSize > -1) {
                        LOG.info()
                                .$("open partition [path=").$substr(dbRootSize, path)
                                .$(", rowCount=").$(partitionSize)
                                .$(", partitionIndex=").$(partitionIndex)
                                .$(", partitionCount=").$(partitionCount)
                                .$(", format=parquet")
                                .I$();

                        final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, partitionNameTxn);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp, cellKey));
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT, PartitionFormat.PARQUET);

                        final long parquetFileSize = openParquetMetadata(partitionIndex, partitionNameTxn);
                        path.trimTo(rootLen);
                        pathGenParquetPartition(partitionIndex, partitionNameTxn);
                        MemoryCMR parquetMem = parquetPartitions.getQuick(partitionIndex);
                        if (parquetMem != null && parquetMem != NullMemoryCMR.INSTANCE) {
                            parquetMem.of(ff, path.$(), parquetFileSize, parquetFileSize, MemoryTag.MMAP_TABLE_READER);
                        } else {
                            // Don't keep fd around to close/open reconciled parquet partitions instead of mremap'ping them.
                            parquetMem = new MemoryCMRDetachedImpl(ff, path.$(), parquetFileSize, MemoryTag.MMAP_TABLE_READER, false);
                            parquetPartitions.setQuick(partitionIndex, parquetMem);
                        }
                        // Initialize columns and index readers for parquet partitions.
                        // reloadColumnAt() sets columns to null (not NullMemoryCMR) for parquet,
                        // which allows createIndexReaderAt() to open real index
                        // readers from the .k/.v files in the native partition directory.
                        path.trimTo(rootLen);
                        Path nativePath = pathGenNativePartition(partitionIndex, partitionNameTxn);
                        openPartitionColumns(partitionIndex, nativePath, getColumnBase(partitionIndex), partitionSize);
                        // Assign SIZE last, matching the native branch below. If any of the
                        // steps above (openParquetMetadata, parquetMem.of, openPartitionColumns)
                        // throws, the slot stays marked closed (-1) so a retry sees a clean
                        // state instead of a torn "open" slot with null resources.
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, partitionSize);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 1);
                        openPartitionCount++;
                    }
                    // Release native state on the existing decoder but keep the Java instance.
                    // getAndInitParquetPartitionDecoder rebinds via of() on next access, and
                    // of() internally destroys stale state. Avoids a new allocation per reload.
                    ParquetPartitionDecoder parquetMetaDecoder = parquetMetaDecoders.getQuick(partitionIndex);
                    if (parquetMetaDecoder != null) {
                        parquetMetaDecoder.close();
                    }

                    return partitionSize;
                }
            } else { // native partition
                Path path = pathGenNativePartition(partitionIndex, partitionNameTxn);
                if (ff.exists(path.$())) {
                    final long partitionSize = getPartitionRowCountFromMetadata(partitionIndex);
                    if (partitionSize > -1) {
                        LOG.debug()
                                .$("open partition [path=").$substr(dbRootSize, path)
                                .$(", rowCount=").$(partitionSize)
                                .$(", partitionIndex=").$(partitionIndex)
                                .$(", partitionCount=").$(partitionCount)
                                .$(", format=native")
                                .I$();

                        final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN, partitionNameTxn);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp, cellKey));
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT, PartitionFormat.NATIVE);
                        openPartitionColumns(partitionIndex, path, getColumnBase(partitionIndex), partitionSize);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, partitionSize);
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 1);
                        openPartitionCount++;
                    }

                    return partitionSize;
                }
            }
            LOG.error().$("open partition failed, partition does not exist on the disk [path=").$(path).I$();

            if (PartitionBy.isPartitioned(getPartitionedBy())) {
                CairoException exception = CairoException.critical(0).put("Partition '");
                formatErrorPartitionDirName(partitionIndex, exception.message);
                exception.put("' does not exist in table '")
                        .put(tableToken.getTableName())
                        .put("' directory. Run [ALTER TABLE ").put(tableToken.getTableName()).put(" FORCE DROP PARTITION LIST '");
                formatErrorPartitionDirName(partitionIndex, exception.message);
                exception.put("'] to repair the table or the database from the backup.");
                throw exception;
            } else {
                throw CairoException.critical(0).put("Table '").put(tableToken.getTableName())
                        .put("' data directory does not exist on the disk at ")
                        .put(path)
                        .put(". Restore data on disk or drop the table.");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartitionColumns(int partitionIndex, Path path, int columnBase, long partitionRowCount) {
        try {
            for (int i = 0; i < columnCount; i++) {
                if (hasActiveColumns && !activeColumns.get(i)) {
                    continue;
                }
                reloadColumnAt(
                        partitionIndex,
                        path,
                        columns,
                        columnTops,
                        indexes,
                        columnBase,
                        i,
                        partitionRowCount
                );
            }
        } catch (Throwable th) {
            closePartitionColumns(columnBase);
            openPartitionInfo.setQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE, -1);
            throw th;
        }
    }

    private void openSymbolMaps() {
        final int columnCount = metadata.getColumnCount();
        // ensure symbolMapReaders has capacity for columnCount entries
        symbolMapReaders.setPos(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.isSymbol(metadata.getColumnType(i))) {
                // symbolMapReaders is sparse
                symbolMapReaders.set(i, newSymbolMapReader(metadata.getDenseSymbolIndex(i), i));
            }
        }
        // Open the read-side composite interners (dedicated dictionaries + _cell registry), mirroring
        // TableWriter.configureColumnMemory()'s write-side registration. These are first-class _txn
        // symbol maps but own no table column, so they are opened into compositeInternerReaders, never
        // into the column-indexed symbolMapReaders above. The interners are always the LAST
        // (layout.dedicatedCount() + 1) symbol slots in _txn (the writer appends them after every real
        // symbol column, and symbol-column DROP compacts real slots but keeps the interners trailing),
        // so their dense indices are derived from the current getSymbolColumnCount() rather than
        // assumed fixed.
        CompositeInternerLayout layout = CompositeInternerLayout.of(metadata.getPartitionSpec());
        if (layout.hasInterners()) {
            final int dimCount = metadata.getPartitionSpec().getDimensionCount();
            final int internerCount = layout.dedicatedCount() + 1;
            final int n = txFile.getSymbolColumnCount();
            ObjList<SymbolMapReader> dedicatedDictReaders = new ObjList<>(dimCount);
            int s = 0;
            for (int i = 0; i < dimCount; i++) {
                if (layout.needsDedicatedDict(i)) {
                    final int denseIndex = n - internerCount + s;
                    SymbolMapReaderImpl dictReader = new SymbolMapReaderImpl(
                            configuration,
                            path,
                            layout.dictName(i),
                            layout.dictColumnNameTxn(i),
                            txFile.getSymbolValueCount(denseIndex)
                    );
                    compositeInternerReaders.add(dictReader);
                    dedicatedDictReaders.extendAndSet(i, dictReader);
                    s++;
                }
            }
            final int registryDenseIndex = n - 1;
            SymbolMapReaderImpl registryReader = new SymbolMapReaderImpl(
                    configuration,
                    path,
                    CompositeInternerLayout.REGISTRY_NAME,
                    CompositeInternerLayout.REGISTRY_TXN,
                    txFile.getSymbolValueCount(registryDenseIndex)
            );
            compositeInternerReaders.add(registryReader);
            compositeDicts = new CompositeDictionaries(new CellRegistry(registryReader), dedicatedDictReaders);
        }
    }

    private Path pathGenNativePartition(int partitionIndex, long nameTxn) {
        formatNativePartitionDirName(partitionIndex, path.slash(), nameTxn);
        return path;
    }

    private Path pathGenParquetPartition(int partitionIndex, long nameTxn) {
        formatParquetPartitionFileName(partitionIndex, path.slash(), nameTxn);
        return path;
    }

    private Path pathGenParquetPartitionMetadata(int partitionIndex, long nameTxn) {
        formatParquetPartitionMetadataFileName(partitionIndex, path.slash(), nameTxn);
        return path;
    }

    private void readTxnSlow(long deadline) {
        int count = 0;

        while (true) {
            if (txFile.unsafeLoadAll()) {
                // good, very stable, congrats
                long txn = txFile.getTxn();
                releaseTxn();
                this.txn = txn;

                if (acquireTxn()) {
                    this.rowCount = txFile.getFixedRowCount() + txFile.getTransientRowCount();
                    LOG.debug()
                            .$("new transaction [txn=").$(txn)
                            .$(", transientRowCount=").$(txFile.getTransientRowCount())
                            .$(", fixedRowCount=").$(txFile.getFixedRowCount())
                            .$(", maxTimestamp=").$ts(ColumnType.getTimestampDriver(timestampType), txFile.getMaxTimestamp())
                            .$(", attempts=").$(count)
                            .$(", thread=").$(Thread.currentThread().getName())
                            .I$();
                    break;
                }
            }
            // This is unlucky, sequences have changed while we were reading transaction data
            // We must discard and try again
            count++;
            if (clock.getTicks() > deadline) {
                throw CairoException.critical(0).put("Transaction read timeout [src=reader, table=").put(tableToken).put(", timeout=").put(configuration.getSpinLockTimeout()).put("ms]");
            }
            Os.pause();
        }
    }

    private void reconcileOpenPartitions(long prevPartitionVersion, long prevColumnVersion, long prevTruncateVersion) {
        // Reconcile partition full or partial will only update row count of last partition and append new partitions
        boolean truncateHappened = txFile.getTruncateVersion() != prevTruncateVersion;
        if (txFile.getPartitionTableVersion() == prevPartitionVersion && txFile.getColumnVersion() == prevColumnVersion && !truncateHappened) {
            int partitionIndex = Math.max(0, partitionCount - 1);
            final int txPartitionCount = txFile.getPartitionCount();
            if (partitionIndex < txPartitionCount) {
                if (partitionIndex < partitionCount) {
                    final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
                    final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                    // we check that open partition size is non-negative to avoid loading
                    // partition that is not yet in memory
                    if (openPartitionSize > -1) {
                        final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                        final long txPartitionSize = getPartitionRowCountFromMetadata(partitionIndex);
                        final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                        if (openPartitionNameTxn == txPartitionNameTxn) {
                            // We used to skip reloading partition size if the row count is the same and name txn is the same.
                            // But in case of dedup, the row count can be same, but the data can be overwritten by splitting and squashing the partition back
                            // This is ok for fixed size columns but var length columns have to be re-mapped to the bigger / smaller sizes
                            final byte format = getPartitionFormat(partitionIndex);
                            assert format != -1;
                            if (format == PartitionFormat.NATIVE) {
                                if (reloadColumnFiles(partitionIndex, txPartitionSize)) {
                                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                    LOG.debug().$("updated partition size [partition=").$(openPartitionInfo.getQuick(offset)).I$();
                                } else {
                                    closePartition(partitionIndex);
                                }
                            } else {
                                // Parquet _pm and data files are mapped with keepFdOpen=false,
                                // so in-place remap is not possible. Close and re-open on next access.
                                closePartition(partitionIndex);
                            }
                        } else {
                            closePartition(partitionIndex);
                        }
                    }
                    partitionIndex++;
                }
                for (; partitionIndex < txPartitionCount; partitionIndex++) {
                    insertPartition(partitionIndex, txFile.getPartitionTimestampByIndex(partitionIndex), txFile.getPartitionCellKey(partitionIndex));
                }
                reloadSymbolMapCounts();
            }
            return;
        }
        reconcileOpenPartitions0(truncateHappened);
    }

    private void reconcileOpenPartitions0(boolean forceTruncate) {
        int partitionIndex = 0;
        int txPartitionCount = txFile.getPartitionCount();
        int txPartitionIndex = partitionIndex;
        boolean changed = false;
        while (partitionIndex < partitionCount && txPartitionIndex < txPartitionCount) {
            final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
            final long txPartTs = txFile.getPartitionTimestampByIndex(txPartitionIndex);
            final int txPartCellKey = txFile.getPartitionCellKey(txPartitionIndex);
            final long openPartitionTimestamp = openPartitionInfo.getQuick(offset);
            final int openPartitionCellKey = (int) openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_CELL_KEY);

            // Plan 3 Task 7: sorted-unique-key two-pointer merge on the total order (ts, cellKey) --
            // ts primary, cellKey secondary -- NOT ts alone, or two cells sharing a timestamp
            // misclassify one as a "refresh" of the other's physical partition. Plain/dormant tables
            // have cellKey 0 on both sides everywhere, so this reduces exactly to the old ts-only
            // comparison (byte-identical classification, byte-identical behaviour).
            final int cmp = openPartitionTimestamp != txPartTs
                    ? Long.compare(openPartitionTimestamp, txPartTs)
                    : Integer.compare(openPartitionCellKey, txPartCellKey);

            if (cmp < 0) {
                // Deleted partitions
                // This will decrement partitionCount
                closeDeletedPartition(partitionIndex);
            } else if (cmp > 0) {
                // Insert partition
                insertPartition(partitionIndex, txPartTs, txPartCellKey);
                changed = true;
                txPartitionIndex++;
                partitionIndex++;
            } else {
                // Refresh partition -- (ts, cellKey) both match, so this is genuinely the same physical
                // partition on both sides, not merely a same-timestamp coincidence.
                final long txPartitionSize = txFile.getPartitionSize(txPartitionIndex);
                final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                final long openPartitionColumnVersion = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION);

                if (!forceTruncate) {
                    if (openPartitionNameTxn == txPartitionNameTxn && openPartitionColumnVersion == columnVersionReader.getMaxPartitionVersion(txPartTs, txPartCellKey)) {
                        // We used to skip reloading partition size if the row count is the same and name txn is the same.
                        // But in case of dedup, the row count can be same, but the data can be overwritten by splitting and squashing the partition back
                        // This is ok for fixed size columns but var length columns have to be re-mapped to the bigger / smaller sizes
                        if (openPartitionSize > -1) {
                            final byte format = getPartitionFormat(partitionIndex);
                            assert format != -1;
                            if (format == PartitionFormat.NATIVE) {
                                if (reloadColumnFiles(partitionIndex, txPartitionSize)) {
                                    openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE, txPartitionSize);
                                    LOG.debug().$("updated partition size [partition=").$(openPartitionTimestamp).I$();
                                } else {
                                    closePartition(partitionIndex);
                                }
                            } else {
                                // Parquet _pm and data files are mapped with keepFdOpen=false,
                                // so in-place remap is not possible. Close and re-open on next access.
                                closePartition(partitionIndex);
                            }
                        }
                    } else {
                        if (openPartitionSize > -1) {
                            closePartition(partitionIndex);
                        }
                        // Refresh the format even for closed partitions so
                        // getPartitionFormat() returns the correct value after
                        // a CONVERT PARTITION (nameTxn changes on conversion).
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT,
                                txFile.isPartitionParquet(partitionIndex) ? PartitionFormat.PARQUET : PartitionFormat.NATIVE);
                    }
                    changed = true;
                } else if (openPartitionSize > -1 && txPartitionSize > -1) { // Don't force re-open if not yet opened
                    closePartition(partitionIndex);
                }
                txPartitionIndex++;
                partitionIndex++;
            }
        }

        // if while finished on txPartitionIndex == txPartitionCount condition
        // removes deleted opened partitions
        while (partitionIndex < partitionCount) {
            closeDeletedPartition(partitionIndex);
            changed = true;
        }

        // if while finished on partitionIndex == partitionCount condition
        // inserts new partitions at the end
        for (; partitionIndex < txPartitionCount; partitionIndex++) {
            insertPartition(partitionIndex, txFile.getPartitionTimestampByIndex(partitionIndex), txFile.getPartitionCellKey(partitionIndex));
            changed = true;
        }

        if (forceTruncate) {
            reloadAllSymbols();
        } else if (changed) {
            reloadSymbolMapCounts();
        }
    }

    private boolean releaseTxn() {
        if (txnAcquired) {
            long readerCount = txnScoreboard.releaseTxn(id, txn);
            txnAcquired = false;
            return readerCount == 0;
        }
        return false;
    }

    private void reloadAllSymbols() {
        path.trimTo(rootLen);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
                SymbolMapReader symbolMapReader = symbolMapReaders.getQuick(columnIndex);
                if (symbolMapReader instanceof SymbolMapReaderImpl) {
                    final int writerColumnIndex = metadata.getWriterIndex(columnIndex);
                    final long symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(writerColumnIndex);
                    int symbolCount = txFile.getSymbolValueCount(metadata.getDenseSymbolIndex(columnIndex));
                    ((SymbolMapReaderImpl) symbolMapReader).of(configuration, path, metadata.getColumnName(columnIndex), symbolTableNameTxn, symbolCount);
                }
            }
        }
    }

    private void reloadAtTxn(TableReader srcReader, boolean reshuffle) {
        releaseTxn();
        final long txn = srcReader.getTxn();
        if (!txnScoreboard.incrementTxn(id, txn)) {
            throw CairoException.critical(0).put("could not acquire txn for copy, source reader has to be active [table=")
                    .put(tableToken.getTableName()).put(", txn=").put(txn).put(']');
        }
        this.txn = txn;
        txnAcquired = true;
        txFile.loadAllFrom(srcReader.txFile);
        columnVersionReader.readFrom(srcReader.columnVersionReader);
        reloadMetadataFrom(srcReader.metadata, reshuffle);
    }

    private void reloadColumnAt(
            int partitionIndex,
            Path path,
            ObjList<MemoryCMR> columns,
            LongList columnTops,
            ObjList<IndexReader> indexReaders,
            int columnBase,
            int columnIndex,
            long partitionRowCount
    ) {
        final int plen = path.size();
        try {
            final CharSequence name = metadata.getColumnName(columnIndex);
            final int primaryIndex = getPrimaryColumnIndex(columnBase, columnIndex);
            final int secondaryIndex = primaryIndex + 1;
            final long partitionTimestamp = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
            final byte partitionFormat = (byte) openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_FORMAT);
            final long partitionTxn = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_NAME_TXN);
            int writerIndex = metadata.getWriterIndex(columnIndex);
            // Plan 4b Task 2: cell-aware lookup -- a plain 2-arg (cellKey-0-only) lookup here would
            // silently alias a DIFFERENT cell's column-version record whenever this partition's own
            // cellKey is non-zero and shares its timestamp with a sibling cell (see getPartitionCellKey's
            // own docs; byte-identical to before for a plain/dormant table, whose cellKey is always 0).
            final int cellKey = getPartitionCellKey(partitionIndex);
            final int versionRecordIndex = columnVersionReader.getRecordIndex(partitionTimestamp, cellKey, writerIndex);
            final long columnTop = versionRecordIndex > -1 ? columnVersionReader.getColumnTopByIndex(versionRecordIndex) : 0;
            long columnTxn = versionRecordIndex > -1 ? columnVersionReader.getColumnNameTxnByIndex(versionRecordIndex) : -1;
            if (columnTxn == -1) {
                // When a column is added, a column version will have txn number for the partition
                // where it's added. It will also have the txn number in the [default] partition
                columnTxn = columnVersionReader.getDefaultColumnNameTxn(writerIndex);
            }
            final long columnRowCount = partitionRowCount - columnTop;

            // When column is added mid-table existence, the top record is only
            // created in the current partition. Older partitions would simply have no
            // column file. This makes it necessary to check the partition timestamp in Column Version file
            // of when the column was added.
            final boolean hasVersionRecord = versionRecordIndex > -1;
            final long colTopPartTs = columnVersionReader.getColumnTopPartitionTimestamp(writerIndex);
            final boolean isColTopPartTsOk = colTopPartTs <= partitionTimestamp;
            if (columnRowCount > 0 && (hasVersionRecord || isColTopPartTsOk)) {
                if (partitionFormat == PartitionFormat.NATIVE) {
                    final int columnType = metadata.getColumnType(columnIndex);

                    final MemoryCMR dataMem = columns.getQuick(primaryIndex);
                    // We intend to keep the file handle open only for the last partition. All other
                    // partitions will have the file handle closed after memory is mapped. The potential knock-on
                    // effect of that is when user workload is such that it involved appending to non-last partition,
                    // the reader will incur file-reopen and re-map instead of "realloc" call
                    boolean lastPartition = partitionIndex == partitionCount - 1;
                    if (ColumnType.isVarSize(columnType)) {
                        final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                        long auxSize = columnTypeDriver.getAuxVectorSize(columnRowCount);
                        TableUtils.iFile(path.trimTo(plen), name, columnTxn);
                        MemoryCMR auxMem = columns.getQuick(secondaryIndex);
                        // Keep aux files fds open, they are read every time TableReader partition is reopened
                        // to find out what memory to map of the data file.
                        auxMem = openOrCreateColumnMemory(path, columns, secondaryIndex, auxMem, auxSize, lastPartition);
                        long dataSize = columnTypeDriver.getDataVectorSizeAt(auxMem.addressOf(0), columnRowCount - 1);
                        if (dataSize < columnTypeDriver.getDataVectorMinEntrySize() || dataSize >= (1L << 40)) {
                            LOG.critical().$("Invalid var len column size [column=").$safe(name)
                                    .$(", size=").$(dataSize)
                                    .$(", path=").$(path)
                                    .I$();
                            throw CairoException.critical(0).put("Invalid column size [column=").put(path)
                                    .put(", size=").put(dataSize)
                                    .put(']');
                        }
                        TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                        openOrCreateColumnMemory(path, columns, primaryIndex, dataMem, dataSize, lastPartition);
                    } else {
                        TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                        openOrCreateColumnMemory(
                                path,
                                columns,
                                primaryIndex,
                                dataMem,
                                columnRowCount << ColumnType.pow2SizeOf(columnType),
                                lastPartition
                        );
                        Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                    }
                } else {
                    assert partitionFormat == PartitionFormat.PARQUET;
                    Misc.free(columns.getAndSetQuick(primaryIndex, null));
                    Misc.free(columns.getAndSetQuick(secondaryIndex, null));
                }

                columnTops.setQuick(columnBase / 2 + columnIndex, columnTop);

                if (metadata.isColumnIndexed(columnIndex)) {
                    IndexReader indexReader = indexReaders.getQuick(primaryIndex);
                    if (indexReader != null) {
                        indexReader.of(configuration, path.trimTo(plen), name, columnTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp);
                    }
                } else {
                    Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                    Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));
                }
            } else {
                Misc.free(columns.getAndSetQuick(primaryIndex, NullMemoryCMR.INSTANCE));
                Misc.free(columns.getAndSetQuick(secondaryIndex, NullMemoryCMR.INSTANCE));
                // the appropriate index for NUllColumn will be created lazily when requested
                // these indexes have state and may not always be required
                Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                Misc.free(indexReaders.getAndSetQuick(secondaryIndex, null));

                // Column is not present in the partition. Set column top to be the size of the partition.
                columnTops.setQuick(columnBase / 2 + columnIndex, partitionRowCount);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Updates boundaries of all columns in partition.
     *
     * @param partitionIndex index of partition
     * @param rowCount       number of rows in partition
     */
    private boolean reloadColumnFiles(int partitionIndex, long rowCount) {
        int columnBase = getColumnBase(partitionIndex);
        for (int i = 0; i < columnCount; i++) {
            final int index = getPrimaryColumnIndex(columnBase, i);
            MemoryCMR mem1 = columns.getQuick(index);
            if (mem1 == null) {
                continue; // column was never opened — nothing to grow
            }

            long columnFilesRowCount = rowCount - getColumnTop(columnBase, i);
            if (columnFilesRowCount > 0 &&
                    (mem1 == NullMemoryCMR.INSTANCE || !growColumn(
                            (MemoryCMRDetachedImpl) mem1,
                            (MemoryCMRDetachedImpl) columns.getQuick(index + 1),
                            metadata.getColumnType(i),
                            columnFilesRowCount
                    ))) {
                return false;
            }
            closeIndexReader(columnBase, i);
        }
        return true;
    }

    private boolean reloadColumnVersion(long columnVersion, long deadline) {
        if (columnVersionReader.getVersion() != columnVersion) {
            columnVersionReader.readSafe(clock, deadline);
        }
        return columnVersionReader.getVersion() == columnVersion;
    }

    private boolean reloadMetadata(int txnMetadataVersion, long deadline, boolean reshuffleColumns) {
        // create transition index, which will help us reuse already open resources
        if (txnMetadataVersion == metadata.getMetadataVersion()) {
            return true;
        }

        while (true) {
            try {
                if (!metadata.prepareTransition(txnMetadataVersion)) {
                    if (clock.getTicks() < deadline) {
                        return false;
                    }
                    throw CairoException.critical(0).put("Metadata read timeout [src=reader, timeout=").put(configuration.getSpinLockTimeout()).put("ms]");
                }
            } catch (CairoException ex) {
                // This is a temporary solution until we can get multiple versions of metadata not overwriting each other
                TableUtils.handleMetadataLoadException(tableToken, deadline, ex, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
                continue;
            }

            assert !reshuffleColumns || metadata.getColumnCount() == this.columnCount;
            final TableReaderMetadataTransitionIndex transitionIndex = metadata.applyTransition();
            if (reshuffleColumns) {
                reshuffleColumns(transitionIndex);
            }
            return true;
        }
    }

    private void reloadMetadataFrom(TableReaderMetadata srcMeta, boolean reshuffleColumns) {
        // create transition index, which will help us reuse already open resources
        if (srcMeta.getMetadataVersion() == metadata.getMetadataVersion()) {
            return;
        }

        assert !reshuffleColumns || metadata.getColumnCount() == this.columnCount;
        final TableReaderMetadataTransitionIndex transitionIndex = metadata.applyTransitionFrom(srcMeta);
        if (reshuffleColumns) {
            reshuffleColumns(transitionIndex);
        }
    }

    private void reloadSlow(boolean reshuffle) {
        final long deadline = clock.getTicks() + configuration.getSpinLockTimeout();
        do {
            // Reload txn
            readTxnSlow(deadline);
            // Reload _meta if the structure version updated, reload _cv if column version updated
        } while (
            // Reload column versions, column version used in metadata reload column shuffle
                !reloadColumnVersion(txFile.getColumnVersion(), deadline)
                        // Start again if _meta with the matching structure version cannot be loaded
                        || !reloadMetadata(txFile.getMetadataVersion(), deadline, reshuffle)
        );
    }

    private void reloadSymbolMapCounts() {
        for (int i = 0; i < columnCount; i++) {
            if (!ColumnType.isSymbol(metadata.getColumnType(i))) {
                continue;
            }
            symbolMapReaders.getQuick(i).updateSymbolCount(txFile.getSymbolValueCount(metadata.getDenseSymbolIndex(i)));
        }
        if (compositeDicts != null) {
            // The interners are always the LAST compositeInternerReaders.size() (== dedicatedCount() +
            // 1) symbol slots in _txn -- recompute the base from the current getSymbolColumnCount() on
            // every reload, since real symbol columns can be added/dropped around them (see
            // openSymbolMaps()), and compositeInternerReaders' own size (fixed for the table's
            // lifetime -- composite dimensions aren't alterable) is exactly the interner count.
            final int internerCount = compositeInternerReaders.size();
            final int base = txFile.getSymbolColumnCount() - internerCount;
            for (int i = 0; i < internerCount; i++) {
                compositeInternerReaders.getQuick(i).updateSymbolCount(txFile.getSymbolValueCount(base + i));
            }
        }
    }

    private void renewSymbolMapReader(SymbolMapReader reader, int columnIndex) {
        if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))) {
            final int writerColumnIndex = metadata.getWriterIndex(columnIndex);
            final long symbolTableNameTxn = columnVersionReader.getSymbolTableNameTxn(writerColumnIndex);
            String columnName = metadata.getColumnName(columnIndex);
            if (!(reader instanceof SymbolMapReaderImpl symbolMapReader)) {
                reader = new SymbolMapReaderImpl(configuration, path.trimTo(rootLen), columnName, symbolTableNameTxn, 0);
            } else {
                // Fully reopen the symbol map reader only when necessary
                if (symbolMapReader.needsReopen(symbolTableNameTxn)) {
                    symbolMapReader.of(configuration, path.trimTo(rootLen), columnName, symbolTableNameTxn, 0);
                }
            }
        } else {
            if (reader instanceof SymbolMapReaderImpl) {
                ((SymbolMapReaderImpl) reader).close();
                reader = null;
            }
        }
        symbolMapReaders.setQuick(columnIndex, reader);
    }

    private void resetAllColumnsOpenFlag() {
        for (int i = 0; i < partitionCount; i++) {
            final int offset = i * PARTITIONS_SLOT_SIZE;
            if (openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE) > -1) {
                openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 0);
            }
        }
    }

    private void reshuffleColumns(TableReaderMetadataTransitionIndex transitionIndex) {
        final int columnCount = metadata.getColumnCount();

        int columnCountShl = getColumnBits(columnCount);
        // when a column is added, we cannot easily reshuffle columns in-place,
        // the reason is that we'd have to create gaps in the column list between
        // partitions. It is possible in theory, but this could be an algo for
        // another day.
        if (columnCountShl > this.columnCountShl) {
            createNewColumnList(columnCount, transitionIndex, columnCountShl);
        } else {
            reshuffleColumns(columnCount, transitionIndex);
        }
        // rearrange symbol map reader list
        reshuffleSymbolMapReaders(transitionIndex, columnCount);
        this.columnCount = columnCount;
        reloadSymbolMapCounts();
    }

    private void reshuffleColumns(int columnCount, TableReaderMetadataTransitionIndex transitionIndex) {
        LOG.debug().$("reshuffling columns file list [table=").$(tableToken).I$();
        int iterateCount = Math.max(columnCount, this.columnCount);

        for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
            int base = getColumnBase(partitionIndex);
            try {
                long partitionRowCount = openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE + PARTITIONS_SLOT_OFFSET_SIZE);
                if (partitionRowCount > -1 && (partitionRowCount = closeRewrittenPartitionFiles(partitionIndex, base)) > -1) {
                    for (int i = 0; i < iterateCount; i++) {
                        final int copyFrom = transitionIndex.getCopyFromIndex(i);

                        if (transitionIndex.closeColumn(i)) {
                            // This column is deleted (not moved).
                            // Close all files
                            closePartitionColumn(base, i);
                        }

                        // We should only remove columns from existing metadata if column count has reduced.
                        // And we should not attempt to reload columns, which have no matches in the metadata
                        if (i < columnCount) {
                            if (copyFrom == i) {
                                // It appears that the column hasn't changed its position. There are three possibilities here:
                                // 1. The column has been forced out of the reader via closeColumnForRemove(). This is required
                                //    on Windows before column can be deleted. In this case, we must check for marker
                                //    instance and the column from disk
                                // 2. The column hasn't been altered, and we can skip to the next column.
                                MemoryMR col = columns.getQuick(getPrimaryColumnIndex(base, i));
                                if (col instanceof NullMemoryCMR || (col != null && !col.isOpen())) {
                                    reloadColumnAt(
                                            partitionIndex,
                                            path,
                                            columns,
                                            columnTops,
                                            indexes,
                                            base,
                                            i,
                                            partitionRowCount
                                    );
                                }
                            } else if (copyFrom > -1) {
                                copyColumns(base, copyFrom, columns, columnTops, indexes, base, i);
                            } else if (copyFrom != Integer.MIN_VALUE) {
                                // new instance
                                reloadColumnAt(
                                        partitionIndex,
                                        path,
                                        columns,
                                        columnTops,
                                        indexes,
                                        base,
                                        i,
                                        partitionRowCount
                                );
                            }
                        }
                    }
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void reshuffleSymbolMapReaders(TableReaderMetadataTransitionIndex transitionIndex, int columnCount) {
        if (columnCount > this.columnCount) {
            symbolMapReaders.setPos(columnCount);
        }

        // index structure is
        // [action: int, copy from:int]

        // action: if -1 then current column in slave is deleted or renamed, else it's reused
        // "copy from" >= 0 indicates that column is to be copied from slave position
        // "copy from" < 0  indicates that column is new and should be taken from updated metadata position
        // "copy from" == Integer.MIN_VALUE  indicates that column is deleted for good and should not be re-added from any source

        for (int i = 0, n = Math.max(columnCount, this.columnCount); i < n; i++) {
            if (transitionIndex.closeColumn(i)) {
                // deleted
                Misc.freeIfCloseable(symbolMapReaders.getAndSetQuick(i, null));
            }

            final int replaceWith = transitionIndex.getCopyFromIndex(i);
            if (replaceWith > -1) {
                SymbolMapReader rdr = symbolMapReaders.getQuick(replaceWith);
                renewSymbolMapReader(rdr, i);
            } else if (replaceWith != Integer.MIN_VALUE) {
                // new instance
                renewSymbolMapReader(null, i);
            }
        }
    }
}
