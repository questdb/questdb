/*******************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.SymbolMapDiffCursor;
import io.questdb.cairo.wal.SymbolMapDiffEntry;
import io.questdb.cairo.wal.WalReader;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.Chars;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Page frame cursor over a single {@code [rowLo, rowHi)} slice of one WAL segment.
 * Opens a {@link WalReader} per {@link #of} call, mmaps the segment's column files,
 * and exposes a single NATIVE-format {@link PageFrame} whose addresses resolve to
 * those mmaps directly. Symbol resolution is delegated to the WalReader, which
 * reads the hardlinked clean-symbol files and the segment's local {@code _event}
 * diffs.
 * <p>
 * The designated timestamp column is special-cased: WAL stores it as a 128-bit
 * (timestamp, rowId) pair, so the cursor extracts 8-byte timestamps into a
 * pinned buffer to match the layout downstream readers expect.
 * <p>
 * Column tops are not considered: WAL segments are always written with a fixed
 * schema, so any schema drift between segments triggers a full recompute in the
 * caller rather than being handled here.
 * <p>
 * This cursor yields at most one frame per call to {@link #of}; iteration of
 * larger-than-page-frame row ranges is not supported yet.
 */
public class WalSegmentPageFrameCursor implements PageFrameCursor {
    private static final long TIMESTAMP_PAIR_BYTES = 16L;
    private final int columnCount;
    private final IntList columnIndexes;
    private final ColumnMapping columnMapping = new ColumnMapping();
    private final IntList columnSizeShifts;
    private final CairoConfiguration configuration;
    private final MemoryCARWImpl extractedTimestampMem;
    private final SingleFrame frame = new SingleFrame();
    private final LongList pageAddresses = new LongList();
    private final LongList pageSizes = new LongList();
    private final ObjList<WalSymbolTable> symbolTables = new ObjList<>();
    // Per-txn SYMBOL key -> value overlay, keyed by base-table writer index
    // (matching columnIndexes). Repopulated per {@link #of} call from the
    // current WAL transaction's SymbolMapDiff. Takes precedence over
    // WalReader.getSymbolValue, which is prone to stale entries when the WAL
    // writer reuses local ids 0..K-1 across transactions (see DataType=
    // WAL_DEDUP_MODE_DEFAULT behavior in WalWriter.getKeyOrNextSymbolKey).
    // A null entry at a given index means that column had no diff in this
    // transaction, so resolution falls through to the reader.
    private final ObjList<DirectSymbolMap> txnSymbolDiffs = new ObjList<>();
    private boolean consumed;
    private WalReader reader;
    private long rowHi;
    private long rowLo;

    public WalSegmentPageFrameCursor(
            @NotNull CairoConfiguration configuration,
            @Transient @NotNull IntList columnIndexes,
            @Transient @NotNull IntList columnSizeShifts
    ) {
        assert columnIndexes.size() == columnSizeShifts.size();
        this.configuration = configuration;
        this.columnCount = columnIndexes.size();
        this.columnIndexes = new IntList(columnCount);
        this.columnIndexes.addAll(columnIndexes);
        this.columnSizeShifts = new IntList(columnCount);
        this.columnSizeShifts.addAll(columnSizeShifts);
        // Pinned scratch buffer for extracted timestamps. 64 KiB base page is
        // small enough to start cheap and doubles as needed.
        this.extractedTimestampMem = new MemoryCARWImpl(
                64L * 1024L,
                Integer.MAX_VALUE,
                MemoryTag.NATIVE_DEFAULT
        );
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        if (!consumed) {
            counter.add(rowHi - rowLo);
        }
    }

    @Override
    public void close() {
        reader = Misc.free(reader);
        Misc.free(extractedTimestampMem);
        Misc.freeObjList(txnSymbolDiffs);
    }

    @Override
    public ColumnMapping getColumnMapping() {
        return columnMapping;
    }

    @Override
    public long getRemainingRowsInInterval() {
        return consumed ? 0L : (rowHi - rowLo);
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return symbolTables.getQuick(columnIndex);
    }

    @Override
    public boolean isExternal() {
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        // The refresh path is single-threaded and WalReader.getSymbolValue() does not
        // mutate state, so returning the cached instance is safe.
        return symbolTables.getQuick(columnIndex);
    }

    @Override
    public @Nullable PageFrame next(long skipTarget) {
        if (consumed || rowHi == rowLo) {
            return null;
        }
        consumed = true;
        return frame;
    }

    /**
     * Opens the WAL segment at {@code <dbRoot>/<tableToken>/<walName>/<segmentId>} and
     * prepares a single-frame view of rows {@code [rowLo, rowHi)}. The segment must
     * have been physically written with at least {@code segmentRowCount} rows.
     * <p>
     * The {@code metadata} argument supplies the {@code writerIndex} for each query
     * column in {@link #getColumnMapping()}; pass the live view's base-table
     * {@link RecordMetadata}.
     * <p>
     * {@code txnDiffs}, when non-null, supplies the current transaction's
     * {@link SymbolMapDiff} entries. The cursor consumes the cursor into a per-column
     * {@code key -> value} overlay that takes precedence over
     * {@link WalReader#getSymbolValue} for this transaction's rows. This is how
     * symbol columns resolve correctly when the WAL writer reuses local ids across
     * transactions (the writer assigns keys {@code initialSymCount + localId} with
     * {@code localId} reset between commits; when {@code initialSymCount} stays at
     * zero because no WAL apply has refreshed the clean count, transactions collide
     * on key 0, and the reader's cumulative map returns the last-written symbol).
     * <p>
     * Reuses internal buffers across calls; callers should {@link #close()} the
     * cursor when all segments have been consumed.
     */
    public WalSegmentPageFrameCursor of(
            @NotNull TableToken tableToken,
            @NotNull CharSequence walName,
            int segmentId,
            long segmentRowCount,
            long rowLo,
            long rowHi,
            @NotNull RecordMetadata metadata,
            @Nullable SymbolMapDiffCursor txnDiffs
    ) {
        assert rowLo >= 0 && rowHi >= rowLo && rowHi <= segmentRowCount;
        // Lazily allocate the WalReader once and rebind per segment. Each of() call opens the
        // segment via dataCursor.of(this) and mmaps the column files; openSegment's finally
        // trims path back to the WAL directory after that, which is also why we don't call it
        // again here.
        if (reader == null) {
            reader = new WalReader(configuration);
        }
        reader.of(tableToken, walName, segmentId, segmentRowCount);
        this.rowLo = rowLo;
        this.rowHi = rowHi;
        buildTxnSymbolDiffs(txnDiffs);
        computeFrame(metadata);
        toTop();
        return this;
    }

    /**
     * Consumes {@code txnDiffs} into {@link #txnSymbolDiffs}, clearing any overlay
     * entries left behind by the previous {@link #of} call. Each entry lands in the
     * map at index {@code diff.getColumnIndex()}, which matches the base-table writer
     * index that {@code LiveViewRefreshJob.buildColumnMappings} stores in
     * {@link #columnIndexes}.
     */
    private void buildTxnSymbolDiffs(@Nullable SymbolMapDiffCursor txnDiffs) {
        for (int i = 0, n = txnSymbolDiffs.size(); i < n; i++) {
            DirectSymbolMap m = txnSymbolDiffs.getQuick(i);
            if (m != null) {
                m.clear();
            }
        }
        if (txnDiffs == null) {
            return;
        }
        SymbolMapDiff diff = txnDiffs.nextSymbolMapDiff();
        while (diff != null) {
            int colIdx = diff.getColumnIndex();
            DirectSymbolMap map = colIdx < txnSymbolDiffs.size() ? txnSymbolDiffs.getQuick(colIdx) : null;
            if (map == null) {
                map = new DirectSymbolMap(256, 8, MemoryTag.NATIVE_DEFAULT);
                txnSymbolDiffs.extendAndSet(colIdx, map);
            }
            SymbolMapDiffEntry entry = diff.nextEntry();
            while (entry != null) {
                // DirectSymbolMap.put copies the CharSequence's bytes off-heap, so the
                // overlay survives past this entry's re-use on the next nextEntry() call.
                map.put(entry.getKey(), entry.getSymbol());
                entry = diff.nextEntry();
            }
            diff = txnDiffs.nextSymbolMapDiff();
        }
    }

    @Override
    public long size() {
        return rowHi - rowLo;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return true;
    }

    @Override
    public void toTop() {
        consumed = false;
    }

    private void computeFrame(RecordMetadata metadata) {
        columnMapping.clear();
        pageAddresses.setPos(2 * columnCount);
        pageSizes.setPos(2 * columnCount);
        if (symbolTables.size() < columnCount) {
            symbolTables.setPos(columnCount);
        }
        extractedTimestampMem.jumpTo(0);

        for (int i = 0; i < columnCount; i++) {
            final int walColumnIndex = columnIndexes.getQuick(i);
            final int columnType = reader.getColumnType(walColumnIndex);
            // (SQL output position i, base-table writer index walColumnIndex). The
            // mapping is stored for downstream consumers (parquet path uses it; the
            // NATIVE WAL path here resolves via pageAddresses directly).
            columnMapping.addColumn(i, walColumnIndex);

            // Matches WalReader.getPrimaryColumnIndex: two slots per column, offset by 2
            // for the implicit (row-id, timestamp) sentinel pair at the start.
            final int dataIdx = walColumnIndex * 2 + 2;
            final MemoryCR colMem = reader.getColumn(dataIdx);

            if (walColumnIndex == reader.getTimestampIndex()) {
                final long dst = extractTimestamps(colMem);
                pageAddresses.setQuick(2 * i, dst);
                pageAddresses.setQuick(2 * i + 1, 0);
                pageSizes.setQuick(2 * i, (rowHi - rowLo) << 3);
                pageSizes.setQuick(2 * i + 1, 0);
            } else if (ColumnType.isVarSize(columnType)) {
                final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
                final MemoryCR auxCol = reader.getColumn(dataIdx + 1);
                final long auxBase = auxCol.getPageAddress(0);
                final long auxOffsetLo = driver.getAuxVectorOffset(rowLo);
                final long auxOffsetHi = driver.getAuxVectorOffset(rowHi);
                // Data size is measured from the full aux vector (offset 0): the aux
                // entries store absolute data offsets, so consumers add them to the
                // data vector's base address. The frame's data address is therefore
                // the full base too, not a slice-relative pointer.
                final long dataSize = rowHi > 0
                        ? driver.getDataVectorSizeAt(auxBase, rowHi - 1)
                        : 0;
                final long dataAddr = dataSize > 0 ? colMem.getPageAddress(0) : 0;
                pageAddresses.setQuick(2 * i, dataAddr);
                pageAddresses.setQuick(2 * i + 1, auxBase + auxOffsetLo);
                pageSizes.setQuick(2 * i, dataSize);
                pageSizes.setQuick(2 * i + 1, auxOffsetHi - auxOffsetLo);
            } else {
                final int sh = columnSizeShifts.getQuick(i);
                assert sh >= 0 : "fixed-size column expects a non-negative size shift";
                final long address = colMem.getPageAddress(0);
                final long offset = rowLo << sh;
                pageAddresses.setQuick(2 * i, address + offset);
                pageAddresses.setQuick(2 * i + 1, 0);
                pageSizes.setQuick(2 * i, (rowHi - rowLo) << sh);
                pageSizes.setQuick(2 * i + 1, 0);
            }

            if (ColumnType.tagOf(columnType) == ColumnType.SYMBOL) {
                WalSymbolTable symTab = symbolTables.getQuick(i);
                if (symTab == null) {
                    symTab = new WalSymbolTable();
                    symbolTables.setQuick(i, symTab);
                }
                DirectSymbolMap diff = walColumnIndex < txnSymbolDiffs.size()
                        ? txnSymbolDiffs.getQuick(walColumnIndex)
                        : null;
                // Pass an empty map as null — valueOf short-circuits to the reader.
                symTab.of(walColumnIndex, reader, (diff != null && diff.size() > 0) ? diff : null);
            } else {
                symbolTables.setQuick(i, null);
            }
        }
    }

    private long extractTimestamps(MemoryCR colMem) {
        final long rowCount = rowHi - rowLo;
        final long bytes = rowCount << 3;
        if (bytes == 0) {
            return 0;
        }
        extractedTimestampMem.extend(bytes);
        extractedTimestampMem.jumpTo(bytes);
        final long src = colMem.getPageAddress(0) + rowLo * TIMESTAMP_PAIR_BYTES;
        final long dst = extractedTimestampMem.getAddress();
        for (long r = 0; r < rowCount; r++) {
            Unsafe.getUnsafe().putLong(
                    dst + (r << 3),
                    Unsafe.getUnsafe().getLong(src + r * TIMESTAMP_PAIR_BYTES)
            );
        }
        return dst;
    }

    private static final class WalSymbolTable implements StaticSymbolTable {
        private final DirectString scanView = new DirectString();
        private final DirectString viewA = new DirectString();
        private final DirectString viewB = new DirectString();
        // Per-transaction overlay (key -> symbol) built from the current txn's
        // SymbolMapDiff. Null when the txn has no diff entries for this column;
        // resolution falls straight through to the reader.
        private DirectSymbolMap txnDiff;
        private WalReader reader;
        private int walColumnIndex;

        @Override
        public boolean containsNullValue() {
            return false;
        }

        @Override
        public int getSymbolCount() {
            // The WAL symbol map does not expose an exact count without walking
            // the backing store; consumers of live view refresh only need
            // key->value resolution, so an upper-bound sentinel is enough.
            return Integer.MAX_VALUE;
        }

        // Resolves a constant string to the int key in this segment's symbol space.
        // The per-txn overlay takes precedence over the reader's cumulative map for
        // the same reason {@link #resolve} prefers it: cross-txn local-id collisions
        // can leave stale cumulative entries. Filter Functions like
        // {@link io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory.ConstCheckColumnFunc}
        // call this once at filter init per segment, so the O(N) scan in
        // {@link WalReader#getSymbolKey} is acceptable.
        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_NOT_FOUND;
            }
            if (txnDiff != null) {
                for (int k = 0, n = txnDiff.size(); k < n; k++) {
                    CharSequence v = txnDiff.valueOf(k, scanView);
                    if (v != null && Chars.equals(value, v)) {
                        return k;
                    }
                }
            }
            return reader.getSymbolKey(walColumnIndex, value, scanView);
        }

        public void of(int walColumnIndex, WalReader reader, @Nullable DirectSymbolMap txnDiff) {
            this.walColumnIndex = walColumnIndex;
            this.reader = reader;
            this.txnDiff = txnDiff;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return resolve(key, viewB);
        }

        @Override
        public CharSequence valueOf(int key) {
            return resolve(key, viewA);
        }

        // Check the per-txn overlay first. The reader's cumulative symbol map can have stale
        // entries for colliding local ids across transactions, so we cannot trust it for keys
        // that belong to this transaction's diff. For keys < cleanSymbolCount (loaded from the
        // table's clean symbol files), the overlay does not have them and the reader resolves
        // them correctly.
        private CharSequence resolve(int key, DirectString view) {
            if (txnDiff != null) {
                CharSequence value = txnDiff.valueOf(key, view);
                if (value != null) {
                    return value;
                }
            }
            return reader.getSymbolValue(walColumnIndex, key, view);
        }
    }

    private final class SingleFrame implements PageFrame {

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return pageAddresses.getQuick(2 * columnIndex + 1);
        }

        @Override
        public long getAuxPageSize(int columnIndex) {
            return pageSizes.getQuick(2 * columnIndex + 1);
        }

        @Override
        public BitmapIndexReader getBitmapIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException("bitmap indices are not available on WAL segments");
        }

        @Override
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAddresses.getQuick(2 * columnIndex);
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes.getQuick(2 * columnIndex);
        }

        @Override
        public PartitionDecoder getParquetPartitionDecoder() {
            return null;
        }

        @Override
        public int getParquetRowGroup() {
            return -1;
        }

        @Override
        public int getParquetRowGroupHi() {
            return -1;
        }

        @Override
        public int getParquetRowGroupLo() {
            return -1;
        }

        @Override
        public long getPartitionHi() {
            return rowHi - rowLo;
        }

        @Override
        public int getPartitionIndex() {
            return 0;
        }

        @Override
        public long getPartitionLo() {
            return 0;
        }
    }
}
