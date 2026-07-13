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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.SymbolMapDiffCursor;
import io.questdb.cairo.wal.SymbolMapDiffEntry;
import io.questdb.cairo.wal.WalReader;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.Vect;
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
 * schema. When a segment's schema has drifted from the caller's compile-time
 * projection (a referenced base column retyped/dropped/renamed), {@link #of}
 * throws {@link TableReferenceOutOfDateException} before mapping the frame, so
 * the caller recompiles rather than reading through a stale column layout.
 * <p>
 * This cursor yields at most one frame per call to {@link #of}; iteration of
 * larger-than-page-frame row ranges is not supported yet.
 */
public class WalSegmentPageFrameCursor implements PageFrameCursor {
    private static final long TIMESTAMP_PAIR_BYTES = 16L;
    private final IntList columnIndexes = new IntList();
    private final ColumnMapping columnMapping = new ColumnMapping();
    private final IntList columnSizeShifts = new IntList();
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
    // Per-column clean symbol count for the current transaction's diff, keyed by
    // base-table writer index (parallel to txnSymbolDiffs). The diff overlay is
    // keyed by the txn's global symbol keys - the contiguous band
    // [cleanSymbolCount, cleanSymbolCount + diff.size()) - so keyOf must probe
    // that band rather than a dense-from-zero range. Only read when the matching
    // overlay is non-empty, where buildTxnSymbolDiffs has just set it.
    private final IntList txnSymbolCleanCounts = new IntList();
    // Number of base-table columns the current of() call projects; rebound on
    // each of() invocation. Internal capacity lists (pageAddresses, pageSizes,
    // symbolTables) grow lazily to match.
    private int columnCount;
    private boolean consumed;
    private WalReader reader;
    private long rowHi;
    private long rowLo;

    public WalSegmentPageFrameCursor(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
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
     * {@code columnIndexes} / {@code columnSizeShifts} define the projection: each
     * SQL output position {@code i} reads base-table writer index
     * {@code columnIndexes[i]} and (for fixed-width columns) shifts row index by
     * {@code columnSizeShifts[i]}. Both lists are copied internally so callers may
     * mutate them between calls. The {@code metadata} argument supplies column types
     * and {@link RecordMetadata#getColumnIndexQuiet}; pass the live view's base-table
     * metadata.
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
     *
     * @throws TableReferenceOutOfDateException when the opened segment's schema no longer
     *                                          matches {@code metadata} - a referenced base
     *                                          column was retyped/dropped/renamed - so the
     *                                          caller must recompile before reading.
     */
    public WalSegmentPageFrameCursor of(
            @NotNull TableToken tableToken,
            @NotNull CharSequence walName,
            int segmentId,
            long segmentRowCount,
            long rowLo,
            long rowHi,
            @NotNull RecordMetadata metadata,
            @Transient @NotNull IntList columnIndexes,
            @Transient @NotNull IntList columnSizeShifts,
            @Nullable SymbolMapDiffCursor txnDiffs
    ) {
        assert rowLo >= 0 && rowHi >= rowLo && rowHi <= segmentRowCount;
        assert columnIndexes.size() == columnSizeShifts.size();
        // Refresh the column-layout snapshot. Internal IntLists are reused across
        // calls; their contents track the most recent of() invocation.
        this.columnIndexes.clear();
        this.columnIndexes.addAll(columnIndexes);
        this.columnSizeShifts.clear();
        this.columnSizeShifts.addAll(columnSizeShifts);
        this.columnCount = columnIndexes.size();
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
        // Guard against a base-schema drift before mapping the frame. columnIndexes /
        // columnSizeShifts were built from the caller's compile-time projection; a
        // referenced base column retyped/dropped/renamed since then (committed but not
        // yet applied) leaves this segment carrying a different layout. Mapping it would
        // deref a missing column or stride a stale width - an OOB native read. The
        // segment's own metadata is authoritative for the bytes about to be read, so
        // reconcile the projection against it by name and bail before computeFrame.
        final RecordMetadata segmentMetadata = reader.getMetadata();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            final int segmentIndex = segmentMetadata.getColumnIndexQuiet(metadata.getColumnName(i));
            if (segmentIndex < 0 || segmentMetadata.getColumnType(segmentIndex) != metadata.getColumnType(i)) {
                throw TableReferenceOutOfDateException.of(tableToken);
            }
        }
        buildTxnSymbolDiffs(txnDiffs);
        computeFrame(metadata);
        toTop();
        return this;
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
        // Clean counts are per-transaction, exactly like the overlays above: a column
        // whose diff is absent from THIS transaction must not keep the previous one's
        // count, or keyOf would bound its scan by a band that does not belong to it.
        for (int i = 0, n = txnSymbolCleanCounts.size(); i < n; i++) {
            txnSymbolCleanCounts.setQuick(i, 0);
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
            // Record the diff's clean symbol count so keyOf can probe the overlay's
            // real key band [cleanSymbolCount, cleanSymbolCount + size).
            txnSymbolCleanCounts.extendAndSet(colIdx, diff.getCleanSymbolCount());
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
            columnMapping.addColumn(i, walColumnIndex, walColumnIndex);

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
                final boolean hasOverlay = diff != null && diff.size() > 0;
                // The clean count stands alone, even with no overlay: a txn that adds no new symbol
                // still emits an empty diff whenever the column has committed symbols
                // (WalEventWriter.writeSymbolMapDiffs, any initialCount > 0), and keyOf needs that
                // count to bound its scan. Forcing 0 here would collapse the scan and stop an
                // ordinary committed-symbol filter resolving at all.
                final int cleanSymbolCount = walColumnIndex < txnSymbolCleanCounts.size()
                        ? txnSymbolCleanCounts.getQuick(walColumnIndex)
                        : 0;
                symTab.of(walColumnIndex, reader, hasOverlay ? diff : null, cleanSymbolCount);
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
        // Pull the 8-byte ts out of each 16-byte (ts, rowId) pair via the SIMD
        // primitive TableWriter / O3CopyJob already use for exactly this gather;
        // indexHi is inclusive, so pass rowCount - 1.
        Vect.copyFromTimestampIndex(src, 0, rowCount - 1, dst);
        return dst;
    }

    private static final class WalSymbolTable implements StaticSymbolTable {
        private final DirectString viewA = new DirectString();
        private final DirectString viewB = new DirectString();
        // Start of the overlay's key band: this txn's diff keys occupy the
        // contiguous range [cleanSymbolCount, cleanSymbolCount + txnDiff.size()).
        private int cleanSymbolCount;
        private WalReader reader;
        // Per-transaction overlay (key -> symbol) built from the current txn's
        // SymbolMapDiff. Null when the txn has no diff entries for this column;
        // resolution falls straight through to the reader.
        private DirectSymbolMap txnDiff;
        private int walColumnIndex;

        @Override
        public boolean containsNullValue() {
            // Sentinel: this table cannot answer the question without walking the
            // backing store. It is safe only because every consumer that branches on
            // containsNullValue (joins, LATEST BY, the excluded-values filter) sits
            // behind a factory shape CairoEngine.validateLiveViewFactory already
            // rejects, so nothing on the refresh path reads it. A future LV query
            // shape that plans off it would read this answer as fact and be wrong: a
            // WAL segment can carry null symbols. Compute it properly before widening
            // what the LV path asks of this table.
            return false;
        }

        @Override
        public int getSymbolCount() {
            // Must be the real, finite count: a residual symbol filter enumerates
            // 0..getSymbolCount()-1 at every filter init to pre-resolve its matching
            // keys (LIKE/ILIKE/~ on a SYMBOL column - see
            // AbstractLikeSymbolFunctionFactory and MatchSymbolFunctionFactory). An
            // Integer.MAX_VALUE upper-bound sentinel used to send those loops past the
            // real keys, where valueOf returns null: the contains/regex variants NPE'd
            // and bricked the view, while the null-safe startsWith/endsWith variants
            // spun 2^31 iterations per commit and pinned the refresh worker.
            //
            // The reader's key space is dense from 0 (clean dictionary keys, then each
            // txn's diff keys), and the per-txn overlay re-keys that same band, so the
            // reader's count already covers the overlay. Take the max regardless: the
            // count must never cut the current txn's band short, or a filter would drop
            // rows whose symbol key sits past it.
            //
            // This deliberately OVER-covers: the cumulative count also spans sibling txns' dirty
            // bands, which no row of this txn carries. An enumerating filter just resolves a few
            // keys that match nothing here - wasted work, never a wrong answer (resolve() is
            // overlay-first). Do NOT tighten to the exact band without re-checking keyOf: the
            // over-count is harmless, an under-count drops rows.
            int count = reader.getSymbolCount(walColumnIndex);
            if (txnDiff != null) {
                count = Math.max(count, cleanSymbolCount + txnDiff.size());
            }
            return count;
        }

        // Resolves a constant string to the int key in this segment's symbol space.
        // The per-txn overlay takes precedence over the reader's cumulative map for
        // the same reason {@link #resolve} prefers it: cross-txn local-id collisions
        // can leave stale cumulative entries. Filter Functions like
        // {@link io.questdb.griffin.engine.functions.eq.EqSymStrFunctionFactory.ConstCheckColumnFunc}
        // call this at filter init per transaction and segment; both maps lazily
        // build reverse indexes that retain their explicit WAL keys.
        @Override
        public int keyOf(CharSequence value) {
            if (value == null) {
                return SymbolTable.VALUE_NOT_FOUND;
            }
            if (txnDiff != null) {
                // The overlay's keys are this txn's global symbol keys: the contiguous
                // band [cleanSymbolCount, cleanSymbolCount + size), not a dense range
                // from zero. Probe that band and return the actual key. The WAL writer
                // resets local ids per commit, so when the base already holds committed
                // symbols (cleanSymbolCount > 0) two un-applied commits can assign the
                // same key to different values; the reader's cumulative map then keeps
                // only the last-written value, and the per-txn overlay is what shadows
                // that staleness for this txn's rows. valueOf is a keyed lookup, so a
                // dense-from-zero scan would probe absent keys, miss, and fall through
                // to the stale reader map.
                final int key = txnDiff.keyOf(value, cleanSymbolCount, cleanSymbolCount + txnDiff.size());
                if (key > -1) {
                    return key;
                }
            }
            // Falls through for clean symbols (below the overlay band), which the reader resolves
            // correctly. The lookup MUST stop at cleanSymbolCount: above it the cumulative map holds
            // every OTHER txn's dirty band, and local ids restart per commit - so an unbounded lookup
            // could return a key valid in a sibling txn, and every row of THIS txn carrying that
            // same local id would match, admitting rows that violate the view's own WHERE.
            return reader.getSymbolKey(walColumnIndex, value, cleanSymbolCount);
        }

        public void of(int walColumnIndex, WalReader reader, @Nullable DirectSymbolMap txnDiff, int cleanSymbolCount) {
            this.walColumnIndex = walColumnIndex;
            this.reader = reader;
            this.txnDiff = txnDiff;
            this.cleanSymbolCount = cleanSymbolCount;
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
        public int getColumnCount() {
            return columnCount;
        }

        @Override
        public byte getFormat() {
            return PartitionFormat.NATIVE;
        }

        @Override
        public IndexReader getIndexReader(int columnIndex, int direction) {
            throw new UnsupportedOperationException("bitmap indices are not available on WAL segments");
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
