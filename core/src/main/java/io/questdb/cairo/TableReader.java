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
import io.questdb.cairo.idx.AbstractParquetPostingIndexReader;
import io.questdb.cairo.idx.IndexBwdNullReader;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.IndexFwdNullReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexUtils;
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
import io.questdb.std.str.Path;
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
    private static final int PARTITIONS_SLOT_SIZE = 8; // must be power of 2
    // Stride of one cached covering-index entry in the per-partition LongList
    // built by cacheParquetIndexForms: column id (the writer index), index txn,
    // _im file size.
    private static final int PIDX_FORM_ENTRY_SIZE = 3;
    private static final int PIDX_FORM_IM_FILE_SIZE_OFF = 2;
    private static final int PIDX_FORM_INDEX_TXN_OFF = 1;
    private static final int PARTITIONS_SLOT_SIZE_MSB = Numbers.msb(PARTITIONS_SLOT_SIZE);
    private final BitSet activeColumns = new BitSet();
    private final MillisecondClock clock;
    private final ColumnVersionReader columnVersionReader;
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
    private boolean hasActiveColumns;
    // Memo for hasPostingIndexedColumn, keyed on the metadata version it was
    // computed from. -1 means "not computed".
    private boolean hasPostingIndexedColumn;
    private ObjList<IndexReader> indexes;
    private int openPartitionCount;
    private LongList openPartitionInfo;
    // The on-disk form of every covering index a partition's _pm publishes,
    // resolved once at partition-open time from this reader's OWN _pm mapping
    // and read by getPartitionIndexForm / getPartitionIndexTxn /
    // getPartitionIndexImFileSize. One LongList per partition, holding
    // PIDX_FORM_ENTRY_SIZE-long entries; null (or empty) means "this partition
    // publishes no covering index", which is every column native.
    //
    // Deliberately indexed by partition and keyed WITHIN a partition by column
    // id (the writer index), not laid out densely over the (partition, column)
    // grid that `columns`, `indexes` and `columnTops` use:
    //
    //   - It is a projection of the _pm mapping, so it is maintained at exactly
    //     the sites that create, replace and drop that mapping -- and this list
    //     shifts with `parquetMetadataPartitions`, its only two shift sites
    //     being insertPartition and closeDeletedPartition. A dense (partition,
    //     column) list would additionally have to be rebuilt by
    //     createNewColumnList and reshuffled by reshuffleColumns, neither of
    //     which touches the _pm at all.
    //   - Column ids survive a column reshuffle; column indexes do not. An
    //     ALTER TABLE DROP COLUMN shifts every later column index down without
    //     changing any partition's name txn, so closeRewrittenPartitionFiles
    //     leaves the partitions open and a dense per-column-index cache would
    //     silently re-point at its neighbour's entry. The _pm records a column
    //     id for exactly this reason, and so does this.
    private ObjList<LongList> parquetIndexForms;
    private ObjList<ParquetPartitionDecoder> parquetMetaDecoders;
    private ObjList<MemoryCMR> parquetMetadataPartitions;
    private ObjList<MemoryCMR> parquetPartitions;
    // Memo for hasParquetPartitions(long), keyed on the partition table version
    // it was computed from. -1 means "not computed".
    private long parquetPartitionsPartitionTableVersion = -1;
    private boolean parquetPartitionsPresent;
    private int partitionCount;
    private long postingIndexedColumnMetadataVersion = -1;
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
            txFile = new TxReader(ff).ofRO(
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
            txFile = new TxReader(ff).ofRO(
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
            decoder = configuration.newParquetPartitionDecoder();
            parquetMetaDecoders.setQuick(partitionIndex, decoder);
        }
        long parquetMetaAddr = getParquetMetadataAddr(partitionIndex);
        long parquetMetaSize = getParquetMetadataSize(partitionIndex);
        long parquetAddr = getParquetAddr(partitionIndex);
        long parquetSize = getParquetFileSize(partitionIndex);
        if (decoder.getParquetMetaAddr() != parquetMetaAddr || decoder.getParquetMetaSize() != parquetMetaSize) {
            final long timestamp = getPartitionTimestamp(partitionIndex);
            decoder.of(parquetMetaAddr, parquetMetaSize, parquetAddr, parquetSize,
                    tableToken, partitionBy, timestampType, timestamp,
                    MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
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

    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    public long getDataVersion() {
        return txFile.getDataVersion();
    }

    /**
     * The index reader for {@code columnIndex} in {@code partitionIndex},
     * dispatched on the partition's ON-DISK index form.
     * <p>
     * <b>The decision is the published token, not the configured format.</b>
     * {@code cairo.posting.index.parquet.partition.format} says what the NEXT
     * seal will write; it says nothing about what this partition already
     * carries, and the two disagree in both directions. Flip the property to
     * {@code parquet} over a natively sealed partition and a format-keyed
     * dispatch sends a read the native chain would have served correctly to a
     * reader with no artifacts to open; flip it back to {@code native} over a
     * parquet-sealed one and a format-keyed dispatch serves it from a chain the
     * seal left with no visible generation, which a native reader reads as "no
     * keys, no rows" -- a silent empty result rather than an error. So this
     * dispatches on {@link #getPartitionIndexForm}, and never on the property.
     * <p>
     * Decided from <b>this reader's own {@code _pm} mapping</b>, the one
     * {@link #openParquetMetadata} took at partition-open time and sized from
     * the header this snapshot saw. That is what makes the answer this
     * snapshot's answer. A fresh open would not: a token publish restates the
     * same {@code data.parquet} size, so its footer shadows the prior one, and
     * {@code resolveFooter} -- which walks back from the mapped tail and returns
     * the newest match -- would hand a pinned reader the writer's latest
     * {@code index_txn} rather than the one its own snapshot names. Both footers
     * stay in the file; only a mapping taken before the header patch can still
     * select the older, and this reader may hold exactly such a mapping.
     * <p>
     * A cached reader is invalidated on a moved {@code index_txn} as well as on
     * a moved {@code columnNameTxn} / {@code partitionTxn}: a token-only publish
     * moves neither of the latter two, which is precisely why the publish has to
     * bump the partition table version to be noticed at all. A cached reader
     * whose CLASS no longer matches the form is dropped and rebuilt, since a
     * reseal can move a partition from one form to the other without touching
     * anything else about it.
     */
    public IndexReader getIndexReader(int partitionIndex, int columnIndex, int direction) {
        resolvePartitionIndexForm(partitionIndex, columnIndex);
        final int columnBase = getColumnBase(partitionIndex);
        final int index = getPrimaryColumnIndex(columnBase, columnIndex);
        final long partitionTimestamp = txFile.getPartitionTimestampByIndex(partitionIndex);
        final long columnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, metadata.getWriterIndex(columnIndex));
        final long partitionTxn = txFile.getPartitionNameTxn(partitionIndex);
        final boolean parquetForm = getPartitionIndexForm(partitionIndex, columnIndex) == PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET;
        final long indexTxn = getPartitionIndexTxn(partitionIndex, columnIndex);
        IndexReader indexReader = getIndexReaderIfExists(partitionIndex, columnIndex, direction);
        if (indexReader != null) {
            // Single choke point for refreshing the scoreboard pin on cached
            // readers. TableReader.txn advances through several paths
            // (goActive / reload / ...); setting it here covers all of them.
            indexReader.setPinnedTableTxn(txn);
            final boolean parquetReader = indexReader instanceof AbstractParquetPostingIndexReader;
            if (parquetForm != parquetReader) {
                // A reseal moved the partition between forms. The two readers
                // are different classes, so this cannot be rebound: drop it and
                // build the right one.
                Misc.free(indexes.getAndSetQuick(direction == IndexReader.DIR_BACKWARD ? index : index + 1, null));
                return createIndexReaderAt(index, columnBase, columnIndex, columnNameTxn, direction, partitionTxn);
            }
            if (
                    !indexReader.isOpen()
                            || indexReader.getColumnTxn() != columnNameTxn
                            || indexReader.getPartitionTxn() != partitionTxn
                            || (parquetReader && ((AbstractParquetPostingIndexReader) indexReader).getIndexTxn() != indexTxn)
            ) {
                int plen = path.size();
                try {
                    if (parquetReader) {
                        // The nine-argument of() carries no index txn and so
                        // cannot name the artifact pair; ofParquet does.
                        ((AbstractParquetPostingIndexReader) indexReader).ofParquet(
                                configuration,
                                pathGenNativePartition(partitionIndex, partitionTxn),
                                metadata.getColumnName(columnIndex),
                                columnNameTxn,
                                partitionTxn,
                                getColumnTop(columnBase, columnIndex),
                                metadata,
                                columnVersionReader,
                                partitionTimestamp,
                                indexTxn,
                                getPartitionIndexImFileSize(partitionIndex, columnIndex)
                        );
                        // ofParquet rebinds from scratch, so the pin set above
                        // has to be restated.
                        indexReader.setPinnedTableTxn(txn);
                    } else {
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
                    }
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
     * Returns the parquet file size recorded in {@code _txn} for this
     * partition, which must be parquet-format: the format bit is the source
     * of truth and {@link TxReader#getPartitionParquetFileSize(int)} asserts
     * it. Reading from {@link TxReader} keeps the size authoritative when the
     * local {@code data.parquet} mapping is a {@link NullMemoryCMR} (e.g. the
     * file has been removed under the reader): {@code _txn} still records
     * the size the file had at commit time, which is what callers like the
     * parquet decoder's footer resolver need.
     */
    public long getParquetFileSize(int partitionIndex) {
        return txFile.getPartitionParquetFileSize(partitionIndex);
    }

    public long getParquetMetadataAddr(int partitionIndex) {
        MemoryCMR mem = parquetMetadataPartitions.getQuick(partitionIndex);
        return mem != null && mem.isOpen() ? mem.addressOf(0) : 0;
    }

    public long getParquetMetadataSize(int partitionIndex) {
        MemoryCMR mem = parquetMetadataPartitions.getQuick(partitionIndex);
        return mem != null && mem.isOpen() ? mem.size() : 0;
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

    /**
     * The on-disk form of {@code columnIndex}'s covering index in
     * {@code partitionIndex}, as this reader's snapshot publishes it:
     * {@link PostingIndexUtils#PARQUET_INDEX_FORMAT_PARQUET} when the
     * partition's {@code _pm} names a covering-index artifact pair for the
     * column, {@link PostingIndexUtils#PARQUET_INDEX_FORMAT_NATIVE} otherwise --
     * which covers a native partition, a parquet partition whose {@code _pm}
     * publishes nothing for the column, and a column that is not indexed at all.
     * <p>
     * Resolved once at partition-open time by {@link #cacheParquetIndexForms}.
     * Answering NATIVE for a partition that is not open is correct rather than
     * merely convenient: nothing is published for a partition this reader has
     * not mapped, and the callers that must distinguish "no covering index" from
     * "not looked yet" open the partition first --
     * {@link #checkPostingIndexIsReadable} does exactly that.
     * <p>
     * <b>The answer is the published token, never the configured format.</b>
     * {@code cairo.posting.index.parquet.partition.format} says what the NEXT
     * seal will write; it says nothing about what this partition already
     * carries, and the two disagree in both directions. Flip the property to
     * {@code parquet} over a natively sealed partition and a format-keyed
     * decision refuses -- or misdispatches -- a read that the native chain would
     * have served correctly. Flip it back to {@code native} over a
     * parquet-sealed one and a format-keyed decision waves through a native read
     * of a chain the seal left with no visible generation, which answers "no
     * keys, no rows": a silent empty result rather than an error. Dispatch on
     * this method, not on the property.
     * <p>
     * <b>The answer is exactly as pinned as the mapping, and no more.</b> Two
     * bounds a dispatch must not assume away. Both are older than this cache and
     * neither is changed by it, but both survive it:
     * <ul>
     *     <li>A partition opened LAZILY, after a token publish, maps the current
     *     {@code _pm}. A token-only publish restates the same
     *     {@code data.parquet} size, so {@code resolveFooter} matches the newest
     *     footer and such a reader gets the writer's latest {@code index_txn}
     *     even though its {@code _txn} is older. Only a mapping taken BEFORE the
     *     header patch still selects the older footer.</li>
     *     <li>{@link #closeExcessPartitions()} -- max-open-partition eviction,
     *     and {@link #goPassive()} -- can close and re-open a partition inside
     *     ONE txn, which re-resolves the mapping and this cache with it. A
     *     reader holding one txn is not thereby holding one answer.</li>
     * </ul>
     */
    public byte getPartitionIndexForm(int partitionIndex, int columnIndex) {
        return indexFormEntryOffset(partitionIndex, columnIndex) < 0
                ? PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE
                : PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET;
    }

    /**
     * The size of the {@code _im} sidecar the published covering-index token
     * names for {@code columnIndex} in {@code partitionIndex}, or 0 when
     * {@link #getPartitionIndexForm} is native.
     */
    public long getPartitionIndexImFileSize(int partitionIndex, int columnIndex) {
        final int offset = indexFormEntryOffset(partitionIndex, columnIndex);
        return offset < 0 ? 0 : parquetIndexForms.getQuick(partitionIndex).getQuick(offset + PIDX_FORM_IM_FILE_SIZE_OFF);
    }

    /**
     * The {@code index_txn} the published covering-index token names for
     * {@code columnIndex} in {@code partitionIndex}, or -1 when
     * {@link #getPartitionIndexForm} is native. It names the artifact pair this
     * snapshot is entitled to read, which is not necessarily the writer's
     * latest -- see {@link #checkPostingIndexIsReadable}.
     */
    public long getPartitionIndexTxn(int partitionIndex, int columnIndex) {
        final int offset = indexFormEntryOffset(partitionIndex, columnIndex);
        return offset < 0 ? -1 : parquetIndexForms.getQuick(partitionIndex).getQuick(offset + PIDX_FORM_INDEX_TXN_OFF);
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
     * <p>
     * Clear?
     *
     * @param partitionIndex the index of the partition in question
     * @return upper bound of the timestamp that can possibly be stored in this partition, which is an inclusive value.
     */
    public long getPartitionMaxTimestampFromMetadata(int partitionIndex) {
        int next = partitionIndex + 1;
        long minTimestampCeil = txFile.getNextLogicalPartitionTimestamp(getPartitionMinTimestampFromMetadata(partitionIndex));
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

    /**
     * Drops the cached on-disk index forms of {@code partitionIndex}. Must be
     * called wherever this reader's {@code _pm} mapping for the partition is
     * dropped or replaced -- the cache is a projection of that mapping's
     * resolved footer and means nothing without it. Repopulated by
     * {@link #cacheParquetIndexForms} the next time the partition is opened.
     * <p>
     * Private on purpose: the cache is a projection of a mapping this class
     * owns, and every site that drops or replaces that mapping is in this file.
     * An external lever to drop it is only ever a way to desynchronise the two.
     */
    private void invalidateIndexFormCache(int partitionIndex) {
        final LongList forms = parquetIndexForms.getQuick(partitionIndex);
        if (forms != null) {
            // Kept rather than nulled: the same partition slot is reopened over
            // and over, and openParquetMetadata reuses its MemoryCMR for the
            // same reason.
            forms.clear();
        }
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

    @TestOnly
    public boolean isParquetMetaReaderOpen() {
        return parquetMetaReader.isOpen();
    }

    /**
     * The scratch {@link ParquetMetaFileReader} this reader resolves {@code _pm}
     * footers through. Exposed so a test can read its counters -- notably
     * {@link ParquetMetaFileReader#getFooterResolveCount()}, which is how "the
     * index form is resolved once per partition open" is asserted as a syscall
     * count rather than as a duration.
     */
    @TestOnly
    public ParquetMetaFileReader getParquetMetaReaderForTest() {
        return parquetMetaReader;
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

    /**
     * Records every covering-index entry the partition's resolved {@code _pm}
     * footer publishes, so the dispatch that needs them does not re-resolve the
     * footer per {@code getIndexReader} call -- which is per page frame, per
     * column and per KEY.
     * <p>
     * Called from {@link #openParquetMetadata} with the reader already resolved
     * on THIS snapshot's footer, and nowhere else. That is the whole invariant:
     * the cache is taken from the same mapping, at the same instant, as the
     * mapping itself, so it cannot describe a footer this reader does not hold.
     * <p>
     * Entries whose {@code index_txn} is negative are not cached. No writer
     * produces one -- {@code DROP INDEX} removes the entry rather than marking
     * it -- but the refusal this replaced read a negative txn as "native", and
     * dropping such an entry here keeps that reading rather than turning a
     * hypothetical malformed footer into a refusal.
     */
    private void cacheParquetIndexForms(int partitionIndex) {
        // Clear before anything else, including the n == 0 exit.
        //
        // This is defence in depth, NOT a fix for a reachable bug: the sole call
        // site is openParquetMetadata, which calls invalidateIndexFormCache
        // unconditionally a few lines above with nothing in between, so every
        // route arrives here already clear. Removing this clear alone breaks no
        // test, and testATornPartitionOpenDoesNotStrandTheCachedIndexForm fails
        // only when BOTH this and that invalidate are gone -- they are mutually
        // redundant for everything that reaches this method.
        //
        // It is kept because the redundancy is the caller's property, not this
        // method's. A second call site, or a reordering that moves the
        // invalidate, would otherwise reintroduce a stale answer silently: this
        // method appends and indexFormEntryOffset returns the FIRST match, so a
        // survivor would outrank the entry just resolved and hand out a
        // superseded index_txn. The n == 0 exit is the worse shape -- a
        // partition that stops publishing would keep its stale answer whole
        // rather than merely have it shadowed.
        //
        // The two are not redundant everywhere: the invalidate also covers a
        // throw out of ofWithSizeFromHeader, where this method never runs.
        final LongList existing = parquetIndexForms.getQuick(partitionIndex);
        if (existing != null) {
            existing.clear();
        }
        final int n = parquetMetaReader.getCoveringIndexCount();
        if (n == 0) {
            // The DEFAULT format seals natively and publishes nothing, so this
            // is the common exit: no allocation, no list, and every later
            // getPartitionIndexForm on this partition is a null check.
            return;
        }
        LongList forms = existing;
        if (forms == null) {
            forms = new LongList(n * PIDX_FORM_ENTRY_SIZE);
            parquetIndexForms.setQuick(partitionIndex, forms);
        }
        for (int i = 0; i < n; i++) {
            final long indexTxn = parquetMetaReader.getCoveringIndexTxn(i);
            if (indexTxn < 0) {
                continue;
            }
            forms.add(parquetMetaReader.getCoveringIndexColumnId(i));
            forms.add(indexTxn);
            forms.add(parquetMetaReader.getCoveringIndexImFileSize(i));
        }
    }

    /**
     * Makes {@link #getPartitionIndexForm} answer about what is on disk rather
     * than about what this reader has looked at, so the dispatch in
     * {@link #getIndexReader} can key on it.
     * <p>
     * The cache is resolved at partition-open time, so an unopened partition
     * answers NATIVE -- correct for a partition that has none, indistinguishable
     * from "not looked yet" for one that has. Opening the partition here is what
     * removes the ambiguity, and every caller needs it open anyway. The
     * partition is opened rather than a fresh {@code _pm} mapped, because the
     * mapping the dispatch must read is this snapshot's own.
     * <p>
     * Fails closed. On a partition the {@code _txn} says is parquet, an
     * unreadable {@code _pm} or a footer that does not resolve for the committed
     * {@code data.parquet} size is corruption, and dispatching through it lands
     * on a native read of a chain the parquet seal left with no visible
     * generation, i.e. "no keys, no rows" -- a silent empty result rather than
     * an error. So both throw. Only an unopenable (row-less) partition returns
     * without deciding: it has no index files to read either way.
     * <p>
     * A remotely-served partition cannot be turned into an error by those
     * throws, checked rather than assumed. {@code openPartition0}'s parquet
     * branch already requires the LOCAL {@code _pm} to exist before it opens
     * anything, and {@code openParquetMetadata} already throws "failed to
     * resolve footer" for a footer that does not resolve -- both before this
     * probe can run and for remote and local partitions alike. What remote
     * changes is only the {@code data.parquet}: a missing one is tolerated by
     * stubbing {@code parquetPartitions} with {@code NullMemoryCMR}, and this
     * probe does not touch it. So any partition that reaches here with a mapped
     * {@code _pm} has already had its footer resolved, and one whose {@code _pm}
     * is missing or corrupt failed to open earlier with the same verdict.
     * <p>
     * Reachable only for a POSTING-indexed column of a parquet partition, so a
     * native partition costs two comparisons. A parquet one used to cost a full
     * {@code _pm} CRC32 plus three JNI crossings on EVERY call --
     * {@code resolveFooter} re-verifies the checksum because {@code of()} resets
     * {@code checksumVerified}, then the covering-section read and the
     * {@code clear()} each cross again -- and {@code getIndexReader} is called
     * per page frame, per column, and per KEY by the covering factory. Measured
     * at 9.8 us per call on an 8-partition table whose {@code _pm} files were
     * 7,000 B, i.e. the cost of the CRC over the whole {@code _pm} prefix, on a
     * shape the DEFAULT {@code native} format serves today (native sidecars
     * hard-linked into a parquet partition directory).
     * <p>
     * So it decides nothing itself. {@link #cacheParquetIndexForms} resolves the
     * covering section once, inside {@link #openParquetMetadata}, off the
     * mapping it has just taken, and this reads the answer back through
     * {@link #getPartitionIndexForm}. That is a list lookup and a short scan
     * over the partition's covering entries -- of which the DEFAULT format
     * publishes none, so the list is empty and the scan does not run.
     * <p>
     * Resolving at open time is not merely cheaper, it is what makes the answer
     * this snapshot's: the mapping and the answer are taken at the same instant,
     * so there is no window in which one moves without the other. Every question
     * about the answer's staleness reduces to a question about the mapping's,
     * and a stale mapping is exactly what a pinned reader is entitled to.
     * <p>
     * This replaces the per-partition memo of the "no covering index at all"
     * answer that used to live in two words of {@code openPartitionInfo}, keyed
     * on the {@code _pm} mapping size and the committed {@code data.parquet}
     * size. The cache supersedes it outright rather than complementing it: it
     * answers the same question for the same partitions, for every column rather
     * than only for the empty section, and it needs no key at all because it is
     * rebuilt from the mapping whenever the mapping is.
     */
    private void resolvePartitionIndexForm(int partitionIndex, int columnIndex) {
        if (!IndexType.isPosting(metadata.getColumnIndexType(columnIndex))) {
            return;
        }
        if (getPartitionFormatFromMetadata(partitionIndex) != PartitionFormat.PARQUET) {
            return;
        }
        long addr = getParquetMetadataAddr(partitionIndex);
        if (addr == 0) {
            // Not mapped on this reader yet. Open the partition rather than map
            // a fresh copy: the mapping this probe must read is the snapshot's
            // own, and every caller needs the partition open anyway.
            if (openPartition(partitionIndex) < 0) {
                return;
            }
            addr = getParquetMetadataAddr(partitionIndex);
        }
        // Fail-closed, unchanged: a partition the _txn says is parquet and that
        // opened must have a mapped _pm, because openPartition0's parquet branch
        // maps it before it marks the partition open. The footer-does-not-
        // resolve arm of the same guard now throws out of openParquetMetadata,
        // where the resolve happens, and so also precedes this.
        if (addr == 0 || getParquetMetadataSize(partitionIndex) == 0) {
            throw CairoException.critical(0)
                    .put("could not read the parquet metadata of a partition carrying a posting index [table=")
                    .put(tableToken.getTableName())
                    .put(", column=").put(metadata.getColumnName(columnIndex))
                    .put(", partitionTimestamp=").ts(timestampType, txFile.getPartitionTimestampByIndex(partitionIndex))
                    .put(']');
        }
    }

    private void checkSchedulePurgeO3Partitions() {
        // In scoreboard V2, it is cheap to check that the txn released is not the max txn,
        // do it as a first step before more expensive checks.
        if (txnScoreboard.isOutdated(txn)) {
            long partitionTableVersion = txFile.getPartitionTableVersion();
            // Taken before the reload, against this reader's own snapshot, so it
            // can be compared with the fresh one below. It cannot be deferred:
            // unsafeLoadAll overwrites the list it walks.
            //
            // So it is gated instead. This is an O(partitionCount) walk and
            // txnScoreboard.isOutdated(txn) is the COMMON case under continuous
            // ingest, not a rare one -- an earlier comment here claimed the
            // opposite -- so on a table with thousands of parquet partitions and
            // high reader churn it would be a real new per-release cost. The
            // suppression exists for the covering-index token publish, which is
            // the only per-commit bump that moves no partition directory, and
            // only a POSTING-indexed column can produce one. Every other table
            // keeps the behaviour it had before the suppression existed, at zero
            // added cost.
            //
            // A POSTING-indexed column is necessary but NOT sufficient: a token
            // publish also needs a parquet partition to publish into, because the
            // token lives in that partition's _pm. So a POSTING-indexed table with
            // no parquet partition -- the shape this feature area targets under
            // the DEFAULT configuration -- used to pay the walk on every release
            // to suppress something that could never happen. Both conditions are
            // now required.
            //
            // Deliberately NOT keyed on the configured format as well. A DROP
            // INDEX retirement is a token-only publish and can fire after the
            // property has been flipped back to native, so the configuration says
            // nothing about whether a publish is possible; the parquet partition
            // does.
            //
            // Both staleness directions of BOTH predicates are benign: a wrong
            // false only restores the spurious schedule, a wrong true only pays
            // for a fingerprint. Neither can produce a wrong answer. That is what
            // licenses memoising them.
            final boolean suppressible = hasPostingIndexedColumn()
                    && hasParquetPartitions(partitionTableVersion);
            long partitionListFingerprint = suppressible ? partitionListFingerprint() : 0;
            // In scoreboard V2 isTxnAvailable(txn) can be relatively expensive. We do this check at the end.
            if (txFile.unsafeLoadAll() && txFile.getPartitionTableVersion() > partitionTableVersion && txnScoreboard.isTxnAvailable(txn)) {
                if (suppressible && partitionListFingerprint() == partitionListFingerprint) {
                    // The version moved but no partition directory did. This task
                    // means "the partition list moved on while I held this txn, so
                    // directories I pinned may be removable", and there is nothing
                    // for it to remove: an O3 partition purge is keyed on
                    // partition directory and name txn, both unchanged.
                    //
                    // Several writer-side bumps are not partition-list changes --
                    // markPartitionDataChanged, markParquetPartitionRemoteStale,
                    // the squash counter -- and the covering-index token publish
                    // is one of them, on a per-commit trigger. Without this the
                    // queue takes a task per reader release after every such
                    // commit, and under saturation logs a "queue is full" error
                    // for work that would find nothing.
                    return;
                }
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
        parquetIndexForms.remove(partitionIndex);
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
        // The _pm mapping is gone, so what was resolved from it must go with it.
        // This is the single close-path site: closePartitionResources routes
        // both formats here (a partition that transitioned PARQUET -> NATIVE
        // still has parquet resources to release), and
        // closeRewrittenPartitionFiles calls closeParquetPartition directly.
        invalidateIndexFormCache(partitionIndex);
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
        long newNameTxn = txFile.getPartitionNameTxnByPartitionTimestamp(partitionTs);
        long newSize = txFile.getPartitionRowCountByTimestamp(partitionTs);
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
                        txn,
                        getPartitionIndexForm(partitionIndex, columnIndex),
                        getPartitionIndexTxn(partitionIndex, columnIndex),
                        getPartitionIndexImFileSize(partitionIndex, columnIndex)
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

    private void formatErrorPartitionDirName(int partitionIndex, Utf16Sink sink) {
        TableUtils.setSinkForNativePartition(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                -1
        );
    }

    private void formatNativePartitionDirName(int partitionIndex, Path sink, long nameTxn) {
        TableUtils.setPathForNativePartition(
                sink,
                timestampType,
                partitionBy,
                openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE),
                nameTxn
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

    /**
     * Deliberately does NOT invalidate the index form cache, unlike every other
     * site that drops a {@code _pm} mapping. Both callers make that safe by
     * construction rather than by luck: {@code close()} is the end of the
     * reader, and {@code goActiveAtTxn}'s downgrade branch calls {@code init()}
     * immediately after, which reallocates {@code parquetIndexForms} outright.
     * A third caller would have to invalidate.
     */
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
    }

    private void freeTempMem() {
        if (tempMem8b != 0) {
            tempMem8b = Unsafe.free(tempMem8b, Long.BYTES, MemoryTag.NATIVE_TABLE_READER);
        }
    }

    private long getPartitionNameTxn(int partitionIndex) {
        return txFile.getPartitionNameTxn(partitionIndex);
    }

    private long getPartitionTimestamp(int partitionIndex) {
        return openPartitionInfo.getQuick(partitionIndex * PARTITIONS_SLOT_SIZE);
    }

    /**
     * Whether this reader's snapshot has any parquet partition, i.e. whether the
     * covering-index token publish that
     * {@link #checkSchedulePurgeO3Partitions}'s fingerprint comparison exists to
     * suppress has anywhere to publish INTO -- the token lives in a partition's
     * {@code _pm}, so a table with no parquet partition can never produce one.
     * <p>
     * Cached against the partition table version, which every path that turns a
     * partition into parquet or back bumps ({@code TableWriter} at the tail of
     * both conversions), so the walk is paid once per partition-list change
     * rather than once per reader release. Under continuous append-only ingest
     * that version does not move, so this is O(1) in the case the gate exists
     * for. See the call site for why a stale answer in either direction is
     * harmless -- which is what makes caching it safe at all.
     *
     * @param partitionTableVersion this reader's own snapshot's version, read by
     *                              the caller before {@code unsafeLoadAll}
     */
    private boolean hasParquetPartitions(long partitionTableVersion) {
        if (parquetPartitionsPartitionTableVersion != partitionTableVersion) {
            parquetPartitionsPresent = hasParquetPartitions();
            parquetPartitionsPartitionTableVersion = partitionTableVersion;
        }
        return parquetPartitionsPresent;
    }

    /**
     * Whether this table has a POSTING-indexed column, i.e. whether it can
     * produce the covering-index token publish that
     * {@link #checkSchedulePurgeO3Partitions}'s fingerprint comparison exists to
     * suppress. Cached against the metadata version so the walk is paid once per
     * metadata change rather than once per reader release; see the call site for
     * why a stale answer in either direction is harmless.
     */
    private boolean hasPostingIndexedColumn() {
        final long metadataVersion = metadata.getMetadataVersion();
        if (postingIndexedColumnMetadataVersion != metadataVersion) {
            boolean found = false;
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                if (IndexType.isPosting(metadata.getColumnIndexType(i))) {
                    found = true;
                    break;
                }
            }
            hasPostingIndexedColumn = found;
            postingIndexedColumnMetadataVersion = metadataVersion;
        }
        return hasPostingIndexedColumn;
    }

    /**
     * Offset of {@code columnIndex}'s cached covering-index entry within
     * {@code partitionIndex}'s list, or -1 when the partition publishes none for
     * it.
     * <p>
     * Scans, because the entries are keyed by column id and there is one per
     * covering index the partition publishes -- at most one per POSTING-indexed
     * column, and none at all under the default format, where the list is empty
     * and the loop does not run. The list load is what a partition costs when it
     * has no covering index, which is the shape every existing user runs.
     */
    private int indexFormEntryOffset(int partitionIndex, int columnIndex) {
        final LongList forms = parquetIndexForms.getQuick(partitionIndex);
        if (forms == null) {
            return -1;
        }
        final int n = forms.size();
        if (n == 0) {
            return -1;
        }
        final int columnId = metadata.getWriterIndex(columnIndex);
        for (int i = 0; i < n; i += PIDX_FORM_ENTRY_SIZE) {
            if (forms.getQuick(i) == columnId) {
                return i;
            }
        }
        return -1;
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
        // Parallel to parquetMetadataPartitions, and maintained wherever that
        // list is: a partition's index forms are a projection of its _pm.
        parquetIndexForms = new ObjList<>(partitionCount);
        parquetIndexForms.setAll(partitionCount, null);
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
            openPartitionInfo.setQuick(baseOffset, partitionTimestamp);
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_SIZE, -1); // -1 means it is not open
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_NAME_TXN, txFile.getPartitionNameTxn(i));
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp));
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_FORMAT, isParquet ? PartitionFormat.PARQUET : PartitionFormat.NATIVE);
            openPartitionInfo.setQuick(baseOffset + PARTITIONS_SLOT_OFFSET_ACTIVE_COLUMNS_OPEN, 0);
        }
        return openPartitionInfo;
    }

    private void insertPartition(int partitionIndex, long timestamp) {
        final int columnBase = getColumnBase(partitionIndex);
        final int columnSlotSize = getColumnBase(1);

        final int idx = getPrimaryColumnIndex(columnBase, 0);
        columns.insert(idx, columnSlotSize, NullMemoryCMR.INSTANCE);
        indexes.insert(idx, columnSlotSize, null);
        parquetMetadataPartitions.insert(partitionIndex, 1, NullMemoryCMR.INSTANCE);
        // Inserted by shifting the entries above it up, so without this the new
        // partition would inherit its neighbour's cached index forms.
        parquetIndexForms.insert(partitionIndex, 1, null);
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
            long columnSize
    ) {
        // Sequential scan profiles hint the kernel to read ahead and to
        // release page cache after reading, avoiding memory pressure during
        // large scans. closePartitionColumn() applies the matching DONTNEED
        // hint at unmap time.
        final int madviseOpts = scanProfile != ReaderScanProfile.DEFAULT ? Files.POSIX_MADV_SEQUENTIAL : -1;
        // The fd stays open for as long as the mapping stays open (see the caller), so a later,
        // non-overlapping reader that maps the same file can still find it in the FdCache/MmapCache
        // and share the mapping instead of creating its own independent one.
        MemoryCMRDetachedImpl memory;
        if (mem != null && mem != NullMemoryCMR.INSTANCE) {
            memory = (MemoryCMRDetachedImpl) mem;
            memory.of(ff, path.$(), columnSize, columnSize, MemoryTag.MMAP_TABLE_READER, 0, madviseOpts, true);
        } else {
            memory = new MemoryCMRDetachedImpl(ff, path.$(), columnSize, MemoryTag.MMAP_TABLE_READER, true, madviseOpts);
            columns.setQuick(primaryIndex, memory);
        }
        return memory;
    }

    /**
     * Opens (or remaps) the _pm metadata file for the given partition and
     * returns the parquet file size derived from its footer metadata.
     */
    private long openParquetMetadata(int partitionIndex) {
        final long parquetFileSize = txFile.getPartitionParquetFileSize(partitionIndex);
        assert parquetFileSize > 0;

        MemoryCMRDetachedImpl parquetMetaMem;
        final MemoryCMR existing = parquetMetadataPartitions.getQuick(partitionIndex);
        if (existing != null && existing != NullMemoryCMR.INSTANCE) {
            parquetMetaMem = (MemoryCMRDetachedImpl) existing;
        } else {
            parquetMetaMem = new MemoryCMRDetachedImpl();
            parquetMetadataPartitions.setQuick(partitionIndex, parquetMetaMem);
        }
        // The mapping is about to be replaced, so whatever was resolved from the
        // previous one is now about a file this reader no longer holds. Dropped
        // BEFORE ofWithSizeFromHeader, not after: that call close()s the mapping
        // on failure, so a throw out of it would otherwise leave a populated
        // cache describing a mapping that no longer exists. Only the addr == 0
        // fail-closed guard in checkPostingIndexIsReadable hides that today, and
        // the dispatch that replaces it does not repeat the guard.
        invalidateIndexFormCache(partitionIndex);
        parquetMetaMem.ofWithSizeFromHeader(ff, path.$(), MemoryTag.MMAP_PARQUET_METADATA_READER);

        try {
            parquetMetaReader.of(parquetMetaMem.addressOf(0), parquetMetaMem.size());
            if (!parquetMetaReader.resolveFooter(parquetFileSize)) {
                throw CairoException.critical(0).put("invalid _pm file: failed to resolve footer [path=").put(path).put(']');
            }
            cacheParquetIndexForms(partitionIndex);
            return parquetMetaReader.getParquetFileSize();
        } finally {
            // resolveFooter retains a native reader that borrows parquetMetaMem. This reader is
            // only needed to resolve the size, so destroy it before any later close or remap can
            // invalidate the mmap it references.
            parquetMetaReader.clear();
        }
    }

    private long openPartition0(int partitionIndex) {
        final int offset = partitionIndex * PARTITIONS_SLOT_SIZE;
        if (txFile.getPartitionCount() < 2 && txFile.getTransientRowCount() == 0) {
            return -1;
        }

        try {
            path.trimTo(rootLen);
            final long partitionNameTxn = getPartitionNameTxn(partitionIndex);
            if (txFile.isPartitionParquet(partitionIndex)) {
                Path path = pathGenParquetPartitionMetadata(partitionIndex, partitionNameTxn);
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
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp));
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_FORMAT, PartitionFormat.PARQUET);

                        final long parquetFileSize = openParquetMetadata(partitionIndex);
                        path.trimTo(rootLen);
                        pathGenParquetPartition(partitionIndex, partitionNameTxn);
                        if (ff.exists(path.$())) {
                            MemoryCMR parquetMem = parquetPartitions.getQuick(partitionIndex);
                            try {
                                if (parquetMem != null && parquetMem != NullMemoryCMR.INSTANCE) {
                                    parquetMem.of(ff, path.$(), parquetFileSize, parquetFileSize, MemoryTag.MMAP_TABLE_READER);
                                } else {
                                    // Don't keep fd around to close/open reconciled parquet partitions instead of mremap'ping them.
                                    parquetMem = new MemoryCMRDetachedImpl(ff, path.$(), parquetFileSize, MemoryTag.MMAP_TABLE_READER, false);
                                    parquetPartitions.setQuick(partitionIndex, parquetMem);
                                }
                            } catch (CairoException e) {
                                if (!txFile.isPartitionRemote(partitionIndex)) {
                                    throw e;
                                }
                                LOG.error().$("could not open parquet partition [path=").$(path).$(", err=").$safe(e.getFlyweightMessage()).I$();
                                Misc.free(parquetPartitions.getQuick(partitionIndex));
                                parquetPartitions.setQuick(partitionIndex, NullMemoryCMR.INSTANCE);
                            }
                        } else if (txFile.isPartitionRemote(partitionIndex)) {
                            Misc.free(parquetPartitions.getQuick(partitionIndex));
                            parquetPartitions.setQuick(partitionIndex, NullMemoryCMR.INSTANCE);
                        } else {
                            // Local parquet partition whose data.parquet vanished. Fail loudly here
                            // instead of stubbing, which would surface later as an obscure null pointer.
                            throw CairoException.critical(0).put("parquet partition data file missing [path=").put(path).put(']');
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
                        openPartitionInfo.setQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION, columnVersionReader.getMaxPartitionVersion(partitionTimestamp));
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
    }

    /**
     * Identifies the partition list by the only two things an O3 partition purge
     * can act on: which partitions are attached and which directory version each
     * one names. A bump that leaves both alone -- a token publish, a data-changed
     * mark, a squash-counter increment -- produces the same value, and a purge
     * scheduled for it could only find nothing.
     * <p>
     * Deliberately not a proxy for "did anything change": it answers the
     * narrower question the purge task exists to act on, and it is compared
     * against the same reader's own pre-reload snapshot rather than against a
     * latched version word.
     * <p>
     * It is a 64-bit hash, not the list, so a collision is possible and would
     * make a needed schedule look unnecessary. It does not matter: the effect is
     * that this ONE reader release does not queue a discovery task, and the
     * directories stay on disk until the next release, the next partition change
     * or any other reader's release schedules one. A collision delays a purge,
     * it cannot lose data or free something still referenced -- and it takes
     * two distinct partition lists agreeing in all 64 bits.
     */
    private long partitionListFingerprint() {
        final int n = txFile.getPartitionCount();
        long h = n;
        for (int i = 0; i < n; i++) {
            h = h * 31 + txFile.getPartitionTimestampByIndex(i);
            h = h * 31 + txFile.getPartitionNameTxn(i);
        }
        return h;
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
                    insertPartition(partitionIndex, txFile.getPartitionTimestampByIndex(partitionIndex));
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
            final long openPartitionTimestamp = openPartitionInfo.getQuick(offset);

            if (openPartitionTimestamp < txPartTs) {
                // Deleted partitions
                // This will decrement partitionCount
                closeDeletedPartition(partitionIndex);
            } else if (openPartitionTimestamp > txPartTs) {
                // Insert partition
                insertPartition(partitionIndex, txPartTs);
                changed = true;
                txPartitionIndex++;
                partitionIndex++;
            } else {
                // Refresh partition
                final long txPartitionSize = txFile.getPartitionSize(txPartitionIndex);
                final long txPartitionNameTxn = txFile.getPartitionNameTxn(partitionIndex);
                final long openPartitionSize = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_SIZE);
                final long openPartitionNameTxn = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_NAME_TXN);
                final long openPartitionColumnVersion = openPartitionInfo.getQuick(offset + PARTITIONS_SLOT_OFFSET_COLUMN_VERSION);

                if (!forceTruncate) {
                    if (openPartitionNameTxn == txPartitionNameTxn && openPartitionColumnVersion == columnVersionReader.getMaxPartitionVersion(txPartTs)) {
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
            insertPartition(partitionIndex, txFile.getPartitionTimestampByIndex(partitionIndex));
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
            final int versionRecordIndex = columnVersionReader.getRecordIndex(partitionTimestamp, writerIndex);
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
                    // Keep the file handle open for as long as the reader keeps the column mapped, for
                    // every partition, not just the last one. Closing the fd right after mapping (the old
                    // fd-usage optimisation) evicted the FdCache/MmapCache record immediately, so a second
                    // reader that later mapped the same historical partition's file always got its own,
                    // independent mapping instead of sharing the first one. With many long-lived readers
                    // sweeping the same table, those independent mappings add up and can exhaust the
                    // process's virtual address space even though each one individually is cheap.
                    if (ColumnType.isVarSize(columnType)) {
                        final ColumnTypeDriver columnTypeDriver = ColumnType.getDriver(columnType);
                        long auxSize = columnTypeDriver.getAuxVectorSize(columnRowCount);
                        TableUtils.iFile(path.trimTo(plen), name, columnTxn);
                        MemoryCMR auxMem = columns.getQuick(secondaryIndex);
                        // Keep aux files fds open, they are read every time TableReader partition is reopened
                        // to find out what memory to map of the data file.
                        auxMem = openOrCreateColumnMemory(path, columns, secondaryIndex, auxMem, auxSize);
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
                        openOrCreateColumnMemory(path, columns, primaryIndex, dataMem, dataSize);
                    } else {
                        TableUtils.dFile(path.trimTo(plen), name, columnTxn);
                        openOrCreateColumnMemory(
                                path,
                                columns,
                                primaryIndex,
                                dataMem,
                                columnRowCount << ColumnType.pow2SizeOf(columnType)
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
                    if (indexReader instanceof AbstractParquetPostingIndexReader) {
                        // A parquet-form reader is bound to an index_txn, which
                        // this nine-argument of() does not carry and cannot
                        // name the artifact pair without. Drop it instead:
                        // getIndexReader rebuilds it through ofParquet off the
                        // token, and the token itself is re-resolved by the same
                        // partition open that got here. Reached only for a
                        // DIR_BACKWARD reader -- the only direction this slot
                        // holds -- over a parquet-form covering index.
                        Misc.free(indexReaders.getAndSetQuick(primaryIndex, null));
                    } else if (indexReader != null) {
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

    private boolean reloadColumnVersion(long columnVersion) {
        if (columnVersionReader.getVersion() != columnVersion) {
            // A duration, unlike the absolute deadline readTxnSlow() and reloadMetadata() take.
            columnVersionReader.readSafe(clock, configuration.getSpinLockTimeout());
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
                !reloadColumnVersion(txFile.getColumnVersion())
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
