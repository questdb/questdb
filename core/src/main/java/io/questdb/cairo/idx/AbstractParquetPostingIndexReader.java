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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.IndexMetaFileWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.BinarySequence;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

import java.util.Arrays;

/**
 * Reads a covering index that was sealed into a partition's
 * {@code <col>.pidx.<indexTxn>.parquet} plus its {@code <col>.pidx.<indexTxn>._im}
 * sidecar, rather than into the native {@code .pk} / {@code .pv} / {@code .pc*}
 * chain that {@link AbstractPostingIndexReader} serves.
 * <p>
 * The artifact pair is named by an {@code index_txn}, which the nine-argument
 * {@link IndexReader#of} does not carry -- so this reader is bound through
 * {@link #ofParquet} instead, off the token
 * {@code TableReader.getPartitionIndexForm} resolved from the partition's own
 * {@code _pm} mapping at partition-open time. {@link #of} therefore throws.
 * <p>
 * <b>This class answers structure only.</b> The four {@link PostingIndexReader}
 * primitives return their "cannot answer, walk a cursor" sentinels and
 * {@link #getCursor} throws; Phase 2C Task 4 fills them in. The throw is
 * deliberate: an empty cursor here would turn every indexed query over a
 * parquet-sealed partition into a silent empty result, which is exactly the
 * failure the refusal this dispatch replaces existed to prevent.
 */
public abstract class AbstractParquetPostingIndexReader implements PostingIndexReader {
    private static final Log LOG = LogFactory.getLog(AbstractParquetPostingIndexReader.class);
    protected final IndexMetaFileReader imReader = new IndexMetaFileReader();
    protected long columnTop;
    protected long decodedRowCount;
    protected long decodedRowGroupCount;
    protected long indexTxn = -1;
    protected long partitionTimestamp;
    protected long pidxAddr;
    protected long pidxSize;
    private long columnNameTxn = -1;
    protected CharSequence columnName;
    private FilesFacade ff;
    private boolean frozen;
    private long imFileSize;
    private boolean open;
    private long partitionTxn = -1;
    private long pinnedTableTxn = Long.MAX_VALUE;

    @Override
    public void close() {
        open = false;
        if (pidxAddr != 0) {
            ff.munmap(pidxAddr, pidxSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            pidxAddr = 0;
            pidxSize = 0;
        }
        // Releases the _im mapping this reader owns; safe on a reader that was
        // never bound and safe to repeat.
        imReader.clear();
    }

    /**
     * Row groups this reader has actually decoded since it was bound. Pruning
     * is asserted against this rather than against a duration: a latency
     * assertion passes on warm-up while the skip misses entirely.
     */
    public long getDecodedRowGroupCount() {
        return decodedRowGroupCount;
    }

    /**
     * Rows whose VALUES this reader has decoded -- {@code row_id} and any
     * covered columns -- since it was bound. Pruning level 3 is asserted on
     * this rather than on the row-group count, because narrowing inside a
     * packed group leaves the group count unchanged.
     * <p>
     * The {@code key_id} probe that finds the key's range is not counted: it
     * reads one 4-byte column, which is what buys the narrowing, and counting
     * it would make the metric measure the probe rather than the saving.
     */
    public long getDecodedRowCount() {
        return decodedRowCount;
    }

    /**
     * Pruning level 2: true when {@code rowGroup}'s row-id extent does not
     * intersect the caller's {@code [minValue, maxValue]}, so the group holds
     * nothing the cursor could emit and need not be decoded.
     * <p>
     * Row id is monotone in the designated timestamp within a partition, so an
     * interval scan's row-id range maps onto this EXACTLY rather than
     * conservatively -- a group is skipped only when it provably holds no row
     * in range.
     * <p>
     * The extents come from the {@code _im}'s own {@code RG_ROW_ID_MIN} /
     * {@code RG_ROW_ID_MAX} sections, not from the {@code row_id} chunk's
     * parquet statistics, because the sections are written unconditionally
     * while that chunk does not exist at all under the alternative payload
     * kind. Reading the stats would silently lose time pruning for that
     * payload rather than failing.
     */
    protected boolean isRowGroupPruned(int rowGroup, long minValue, long maxValue) {
        return imReader.getRowGroupRowIdMin(rowGroup) > maxValue
                || imReader.getRowGroupRowIdMax(rowGroup) < minValue;
    }

    /**
     * Finds {@code key}'s contiguous row range inside {@code rowGroup},
     * returning {@code Numbers.encodeLowHighInts(lo, hiExclusive)} or
     * {@link IndexMetaFileReader#KEY_ABSENT} when the group holds none.
     * <p>
     * Pruning level 3's EFFECT. The spec's mechanism is Parquet
     * {@code ColumnIndex}/{@code OffsetIndex} page skipping; the seal does not
     * write either -- its writer takes {@code statistics_enabled} and nothing
     * for a page index -- so the indexes are absent from the file rather than
     * merely unreachable from Java, and Phase 3 must add both. What is
     * available is {@code decodeRowGroup(..., rowLo, rowHi)}, whose Rust side
     * skips pages outside the range, so bounding the decode to the key's rows
     * achieves page skipping through the API that exists.
     * <p>
     * The probe decodes ONLY {@code key_id}, four bytes a row, and binary
     * searches it: the group is key-major, so a key's rows are contiguous.
     * That cost buys skipping {@code row_id} and every covered column for the
     * rows belonging to other keys, which in a packed group is most of them.
     */
    protected long keyRowRangeInGroup(CountingCursor probe, int rowGroup, int key, long groupRows) {
        final long keyIdPtr = probe.decodeKeyIdColumn(rowGroup, groupRows);
        long lo = 0;
        long hi = groupRows;
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (Unsafe.getUnsafe().getInt(keyIdPtr + (mid << 2)) < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        final long start = lo;
        hi = groupRows;
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (Unsafe.getUnsafe().getInt(keyIdPtr + (mid << 2)) <= key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (start >= lo) {
            return IndexMetaFileReader.KEY_ABSENT;
        }
        return Numbers.encodeLowHighInts((int) start, (int) lo);
    }

    /**
     * Records a decode. Kept next to the pruning predicate so the counter and
     * the skip cannot drift: a group is counted where it is decoded, never
     * where it is merely visited.
     */
    protected void onRowGroupDecoded() {
        decodedRowGroupCount++;
    }

    /**
     * @param rows rows whose VALUES were decoded, which after level 3 is the
     *             key's slice of the group rather than the whole group.
     */
    protected void onRowGroupDecoded(long rows) {
        decodedRowGroupCount++;
        decodedRowCount += rows;
    }

    /**
     * Rows before {@code columnTop} carry no value and are not in the index at
     * all, so key 0 (NULL) owns them implicitly. Bounded by
     * {@code nullMaxValue}, the UNCLAMPED caller max, because the prefix is
     * independent of the index and of {@code getEntryMaxValue}.
     */
    protected long nullPrefixCount(int key, long nullMaxValue) {
        if (key != 0 || columnTop <= 0 || nullMaxValue < 0) {
            return 0;
        }
        return Math.min(columnTop, nullMaxValue + 1);
    }

    /**
     * True when every row of {@code rowGroup} belongs to {@code key}.
     * <p>
     * The index is key-major, so within a key's run only the FIRST and LAST
     * groups can be shared with a neighbour -- an interior group cannot hold
     * another key without breaking the run. A single-group run is a boundary
     * group on both sides and is never treated as dedicated.
     */
    protected boolean isGroupDedicatedTo(int rowGroup, int key, int rgLo, int rgHi) {
        return rowGroup > rgLo && rowGroup < rgHi;
    }

    /**
     * True when the group's whole row-id extent sits inside the window, so no
     * row of it can be clipped.
     */
    protected boolean isWholeGroupInRange(int rowGroup, long minValue, long maxValue) {
        return imReader.getRowGroupRowIdMin(rowGroup) >= minValue
                && imReader.getRowGroupRowIdMax(rowGroup) <= maxValue;
    }

    /**
     * A cursor used only to count and to pick, never handed to a caller. It
     * exists so the metadata primitives decode through the same per-cursor
     * state everything else does -- its own decoder, buffers and projection --
     * rather than borrowing the pooled cursor, which a concurrent worker may be
     * iterating.
     */
    protected class CountingCursor extends AbstractCoveringCursor {
        @Override
        public void close() {
            freeResources();
        }

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException("counting cursor is not iterable");
        }

        @Override
        public long next() {
            throw new UnsupportedOperationException("counting cursor is not iterable");
        }

        long countInGroup(int rowGroup, int key, long minValue, long maxValue) {
            final long rows = decodeGroup(rowGroup);
            final long keyIdPtr = rowGroupBuffers.getChunkDataPtr(0);
            final long rowIdPtr = rowGroupBuffers.getChunkDataPtr(1);
            long n = 0;
            for (long i = 0; i < rows; i++) {
                if (Unsafe.getUnsafe().getInt(keyIdPtr + (i << 2)) != key) {
                    continue;
                }
                final long rowId = Unsafe.getUnsafe().getLong(rowIdPtr + (i << 3));
                if (rowId >= minValue && rowId <= maxValue) {
                    n++;
                }
            }
            return n;
        }

        long selectInGroup(int rowGroup, int key, long minValue, long maxValue, long j) {
            final long rows = decodeGroup(rowGroup);
            final long keyIdPtr = rowGroupBuffers.getChunkDataPtr(0);
            final long rowIdPtr = rowGroupBuffers.getChunkDataPtr(1);
            long seen = 0;
            for (long i = 0; i < rows; i++) {
                if (Unsafe.getUnsafe().getInt(keyIdPtr + (i << 2)) != key) {
                    continue;
                }
                final long rowId = Unsafe.getUnsafe().getLong(rowIdPtr + (i << 3));
                if (rowId < minValue || rowId > maxValue) {
                    continue;
                }
                if (seen++ == j) {
                    return rowId;
                }
            }
            return Numbers.LONG_NULL;
        }

        long decodeKeyIdColumn(int rowGroup, long rows) {
            projection.clear();
            projection.add(imReader.getKeyIdColumn());
            projection.add(ColumnType.INT);
            rowGroupBuffers.reopen();
            decoder().decodeRowGroup(rowGroupBuffers, projection, rowGroup, 0, (int) rows);
            return rowGroupBuffers.getChunkDataPtr(0);
        }

        private long decodeGroup(int rowGroup) {
            final long rows = imReader.getRowGroupNumRows(rowGroup);
            final DirectIntList columns = coveringProjection(null);
            rowGroupBuffers.reopen();
            decoder().decodeRowGroup(rowGroupBuffers, columns, rowGroup, 0, (int) rows);
            onRowGroupDecoded();
            return rows;
        }
    }

    /**
     * Resolves {@code key} to its inclusive index row-group run through the
     * {@code _im} directory. Pruning level 1: exact, and it reads no byte of
     * the index parquet.
     * <p>
     * The directory answers "which row groups COULD hold k", not "does k
     * exist": the key space is dense and occupancy sparse, so a key falling
     * inside a packed group's key range returns a range whether or not it has
     * postings. Confirming absence costs one row-group decode, which the
     * cursor performs anyway.
     */
    protected long rowGroupRangeForKey(int key) {
        return imReader.getRowGroupRangeForKey(key);
    }

    /**
     * The covered-value half of both cursors: everything depending on the
     * decoded chunks and the current row, but not on traversal order.
     * <p>
     * Subclasses own traversal and must call {@link #setEmittedRow(long)} with
     * the index of the row they are about to return, because every accessor
     * reads that row of the decoded group.
     * <p>
     * Only fixed-width covered types are reachable: the seal refuses
     * {@code isVarSize} and symbol covered columns outright, so a covered
     * string, varchar, binary or array cannot exist in an index parquet. Those
     * accessors throw rather than returning null -- an unreachable branch that
     * returns a plausible value is how a silent wrong answer ships if the
     * seal's restriction is ever relaxed without revisiting this class.
     */
    protected abstract class AbstractCoveringCursor implements CoveringRowCursor {
        // Owned per cursor, not per reader. getDetachedCursor hands N workers N
        // cursors over ONE frozen reader, and they decode concurrently: shared
        // buffers would interleave two groups in one allocation, and a shared
        // slot->ordinal map would have each cursor's projection overwrite the
        // other's. Only the _im and parquet mappings are shared, and those are
        // immutable while frozen.
        // The decoder is per-cursor for the same reason the buffers are, and
        // it is the one that bites: ParquetFileDecoder caches a lazily-created
        // native decode context, so N workers decoding through one instance
        // race on it. That does not fail loudly -- it returns another group's
        // rows, which the concurrency test caught as two cursors disagreeing
        // about a posting 15000 rows in.
        protected final ParquetFileDecoder decoder = new ParquetFileDecoder();
        protected final DirectIntList projection = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT);
        protected final RowGroupBuffers rowGroupBuffers =
                new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
        protected int[] coverChunkOrdinal;
        protected long emittedRow = -1;
        private boolean decoderBound;

        /**
         * Binds this cursor's decoder to the reader's parquet mapping. The
         * mapping is immutable while the reader is bound, so every cursor may
         * hold its own decoder over the same bytes.
         */
        protected ParquetFileDecoder decoder() {
            if (!decoderBound) {
                decoder.of(pidxAddr, pidxSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                decoderBound = true;
            }
            return decoder;
        }

        /**
         * Releases everything this cursor owns. Called when the READER closes
         * for its pooled cursor, and by {@link #close()} for a detached one,
         * which no reader will ever come back for.
         */
        protected void freeResources() {
            Misc.free(decoder);
            Misc.free(rowGroupBuffers);
            Misc.free(projection);
            decoderBound = false;
        }

        /**
         * Builds the decode projection for a covering cursor: {@code key_id},
         * {@code row_id}, then one entry per requested cover slot, and records
         * where each slot's chunk lands.
         * <p>
         * <b>Three index spaces meet here and must not be confused.</b>
         * {@code requiredCoverColumns} are COVER SLOTS.
         * {@link IndexMetaFileReader#getCoverColumnIndex(int)} maps a slot to a
         * DESCRIPTOR INDEX, which is also the parquet column index -- that is what
         * the decoder wants. A descriptor's {@code ID} is the covered column's
         * WRITER INDEX and is not a lookup key on this path at all.
         * <p>
         * Chunk ordinals follow the projection's order, so slot {@code s} lands at
         * {@code 2 + its position in requiredCoverColumns}, never at {@code s}.
         * The two are equal only when the caller asks for a dense prefix of the
         * slots, which is exactly the case a wrong mapping would still pass.
         */
        protected DirectIntList coveringProjection(int[] requiredCoverColumns) {
            projection.clear();
            projection.add(imReader.getKeyIdColumn());
            projection.add(ColumnType.INT);
            projection.add(imReader.getRowIdColumn());
            projection.add(ColumnType.LONG);

            final int coverCount = imReader.getColumnCount() - imReader.getFirstCoverColumn();
            if (coverChunkOrdinal == null || coverChunkOrdinal.length < coverCount) {
                coverChunkOrdinal = new int[Math.max(coverCount, 8)];
            }
            Arrays.fill(coverChunkOrdinal, 0, coverChunkOrdinal.length, -1);
            if (requiredCoverColumns == null) {
                return projection;
            }
            int ordinal = 2;
            for (int i = 0; i < requiredCoverColumns.length; i++) {
                final int slot = requiredCoverColumns[i];
                if (slot < 0 || slot >= coverCount) {
                    throw CairoException.critical(0)
                            .put("cover slot out of range [slot=").put(slot)
                            .put(", coverCount=").put(coverCount)
                            .put(", column=").put(columnName).put(']');
                }
                if (coverChunkOrdinal[slot] >= 0) {
                    continue; // asked for twice; one chunk serves both
                }
                final int descriptor = imReader.getCoverColumnIndex(slot);
                projection.add(descriptor);
                projection.add(imReader.getColumnType(descriptor));
                coverChunkOrdinal[slot] = ordinal++;
            }
            return projection;
        }

        @Override
        public ArrayView getCoveredArray(int includeIdx, int columnType) {
            throw unsupportedCoveredType("ARRAY", includeIdx);
        }

        @Override
        public BinarySequence getCoveredBin(int includeIdx) {
            throw unsupportedCoveredType("BINARY", includeIdx);
        }

        @Override
        public long getCoveredBinLen(int includeIdx) {
            throw unsupportedCoveredType("BINARY", includeIdx);
        }

        @Override
        public byte getCoveredByte(int includeIdx) {
            return Unsafe.getUnsafe().getByte(coveredAddress(includeIdx, 1));
        }

        @Override
        public double getCoveredDouble(int includeIdx) {
            return Unsafe.getUnsafe().getDouble(coveredAddress(includeIdx, 8));
        }

        @Override
        public float getCoveredFloat(int includeIdx) {
            return Unsafe.getUnsafe().getFloat(coveredAddress(includeIdx, 4));
        }

        @Override
        public int getCoveredInt(int includeIdx) {
            return Unsafe.getUnsafe().getInt(coveredAddress(includeIdx, 4));
        }

        @Override
        public long getCoveredLong(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 8));
        }

        @Override
        public long getCoveredLong128Hi(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 16) + 8);
        }

        @Override
        public long getCoveredLong128Lo(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 16));
        }

        @Override
        public long getCoveredLong256_0(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32));
        }

        @Override
        public long getCoveredLong256_1(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32) + 8);
        }

        @Override
        public long getCoveredLong256_2(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32) + 16);
        }

        @Override
        public long getCoveredLong256_3(int includeIdx) {
            return Unsafe.getUnsafe().getLong(coveredAddress(includeIdx, 32) + 24);
        }

        @Override
        public short getCoveredShort(int includeIdx) {
            return Unsafe.getUnsafe().getShort(coveredAddress(includeIdx, 2));
        }

        @Override
        public CharSequence getCoveredStrA(int includeIdx) {
            throw unsupportedCoveredType("STRING", includeIdx);
        }

        @Override
        public CharSequence getCoveredStrB(int includeIdx) {
            throw unsupportedCoveredType("STRING", includeIdx);
        }

        @Override
        public Utf8Sequence getCoveredVarcharA(int includeIdx) {
            throw unsupportedCoveredType("VARCHAR", includeIdx);
        }

        @Override
        public Utf8Sequence getCoveredVarcharB(int includeIdx) {
            throw unsupportedCoveredType("VARCHAR", includeIdx);
        }

        /**
         * A slot is available when the caller asked for it AND a row has been
         * emitted. Answering true for an unrequested slot would hand back
         * another column's bytes, so this keys on the projection actually
         * built, not on what the index happens to cover.
         */
        @Override
        public boolean isCoveredAvailable(int includeIdx) {
            return emittedRow >= 0
                    && coverChunkOrdinal != null
                    && includeIdx >= 0
                    && includeIdx < coverChunkOrdinal.length
                    && coverChunkOrdinal[includeIdx] >= 0;
        }

        /**
         * Refused on a forward cursor, matching
         * {@code AbstractPostingIndexReader}: reaching the last posting
         * forwards is O(n) over the key's whole run, and the caller
         * (LATEST ON's covering path) always has a backward reader available.
         * The backward cursor overrides it.
         */
        @Override
        public long seekToLast() {
            throw new UnsupportedOperationException(
                    "seekToLast: use a backward index reader; forward iteration is O(n)");
        }

        /**
         * Address of {@code includeIdx}'s value for the row last emitted.
         * {@code width} is the fixed element width, which is what makes this a
         * multiply rather than an offset lookup -- correct only because every
         * reachable covered type is fixed-width.
         */
        protected long coveredAddress(int includeIdx, int width) {
            if (!isCoveredAvailable(includeIdx)) {
                throw CairoException.critical(0)
                        .put("covered slot was not projected [slot=").put(includeIdx)
                        .put(", column=").put(columnName).put(']');
            }
            return rowGroupBuffers.getChunkDataPtr(coverChunkOrdinal[includeIdx]) + emittedRow * width;
        }

        protected void setEmittedRow(long row) {
            this.emittedRow = row;
        }

        private CairoException unsupportedCoveredType(String type, int includeIdx) {
            return CairoException.critical(0)
                    .put("parquet covering index does not carry a covered ").put(type)
                    .put(" [slot=").put(includeIdx)
                    .put(", column=").put(columnName)
                    .put("]; the seal refuses var-size and symbol covered columns");
        }
    }

    /**
     * The sentinel is {@link Numbers#LONG_NULL}, NOT {@code -1}: the sole caller
     * tests {@code c != Numbers.LONG_NULL} and then does {@code total += c}, so
     * {@code -1} does not signal a fallback -- it silently subtracts one from a
     * {@code count(*)} answer.
     */
    @Override
    public long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped) {
        if (key < 0 || maxValueClamped < minValue) {
            return Numbers.LONG_NULL;
        }
        long total = nullPrefixCount(key, nullMaxValue);
        final long range = rowGroupRangeForKey(key);
        if (range == IndexMetaFileReader.KEY_ABSENT) {
            return total;
        }
        final int rgLo = Numbers.decodeLowInt(range);
        final int rgHi = Numbers.decodeHighInt(range);
        try (CountingCursor counter = new CountingCursor()) {
            for (int rg = rgLo; rg <= rgHi; rg++) {
                final long rows = imReader.getRowGroupNumRows(rg);
                if (rows <= 0 || isRowGroupPruned(rg, minValue, maxValueClamped)) {
                    continue;
                }
                if (isWholeGroupInRange(rg, minValue, maxValueClamped) && isGroupDedicatedTo(rg, key, rgLo, rgHi)) {
                    // Every row in this group belongs to this key and falls
                    // inside the window, so its row count IS the answer for it.
                    // No decode: this is the whole point of the primitive.
                    total += rows;
                    continue;
                }
                total += counter.countInGroup(rg, key, minValue, maxValueClamped);
            }
        }
        return total;
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnNameTxn;
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        // Direction is the subclass's business: the forward reader overrides
        // this, and the backward one still refuses until Task 6. Leaving the
        // base throwing rather than defaulting to ascending keeps a backward
        // caller from silently receiving forward order.
        throw CairoException.critical(0)
                .put("parquet-form posting index cursor is not implemented for this direction [column=")
                .put(columnName).put(", indexTxn=").put(indexTxn).put(']');
    }

    /**
     * Negative means "no entry". Unlike the two {@code LONG_NULL} sentinels this
     * one IS negative by contract, and {@link AbstractPostingIndexReader} spells
     * it {@code -1}.
     */
    @Override
    public long getEntryMaxValue() {
        // Highest row id the sealed index covers, or -1 when it covers nothing.
        // Negative by contract, and callers branch on the sign to decide
        // whether to clamp their walk -- returning 0 for an empty index would
        // clamp every cursor to row 0 instead of leaving it unclamped.
        final int groups = imReader.getIndexRowGroupCount();
        if (groups <= 0) {
            return -1;
        }
        long max = -1;
        for (int rg = 0; rg < groups; rg++) {
            if (imReader.getRowGroupNumRows(rg) <= 0) {
                continue;
            }
            final long m = imReader.getRowGroupRowIdMax(rg);
            if (m > max) {
                max = m;
            }
        }
        return max;
    }

    /**
     * The committed {@code IM_FILE_SIZE} of the {@code _im} this reader is bound
     * to, as the published token names it.
     */
    public long getImFileSize() {
        return imFileSize;
    }

    /**
     * The {@code index_txn} naming the artifact pair this reader is bound to.
     * {@code TableReader} compares it with the partition's published token to
     * decide whether a cached reader still describes the right generation: a
     * token-only publish moves neither {@code columnNameTxn} nor
     * {@code partitionTxn}, so nothing else here would notice it.
     */
    public long getIndexTxn() {
        return indexTxn;
    }

    /**
     * Native-mmap-shaped and meaningless for a parquet-backed reader, so
     * {@code 0}. Audited callers, both of which tolerate it:
     * {@code LatestByAllIndexedRecordCursor}, whose factory is gated on
     * {@code IndexType.BITMAP} at both construction sites in
     * {@code SqlCodeGenerator} so a POSTING reader never reaches it; and
     * {@code TouchTableFunctionFactory}, whose {@code touchMemory} returns 0
     * pages for {@code baseAddress == 0}, degrading {@code touch_table()} over
     * this index to a no-op rather than dereferencing anything.
     */
    @Override
    public long getKeyBaseAddress() {
        return 0;
    }

    @Override
    public int getKeyCount() {
        // KEY_SPACE_SIZE: the exclusive upper bound on key ids, equal to the
        // native reader's keyCountIncludingNulls. NOT a distinct-key count --
        // occupancy is sparse, and a distinct count would make every key above
        // the first report absent with no error anywhere.
        return imReader.getKeySpaceSize();
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getKeyMemorySize() {
        return 0;
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    /**
     * The table {@code _txn} this reader is pinned at, for the snapshot
     * isolation Task 4 onwards needs.
     */
    public long getPinnedTableTxn() {
        return pinnedTableTxn;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getValueBaseAddress() {
        return 0;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getValueMemorySize() {
        return 0;
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Always throws. The nine-argument form carries no {@code index_txn} and so
     * cannot name {@code <col>.pidx.<indexTxn>.parquet} or its {@code _im}; use
     * {@link #ofParquet} instead. Unreachable from production --
     * {@code TableReader.getIndexReader} rebinds a parquet-form reader through
     * {@code ofParquet}, and {@code reloadColumnAt} drops one rather than
     * rebinding it -- so this is a programming-error guard, not a code path.
     */
    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        throw CairoException.critical(0)
                .put("a parquet-form posting index reader must be bound through ofParquet, which carries the index txn [column=")
                .put(columnName).put(']');
    }

    /**
     * Binds this reader to {@code <col>.pidx.<indexTxn>._im} and
     * {@code <col>.pidx.<indexTxn>.parquet} in the partition directory
     * {@code path} names.
     *
     * @param path       positioned at the partition directory; restored on return
     * @param indexTxn   the {@code index_txn} the partition's {@code _pm} publishes
     *                   for this column
     * @param imFileSize the {@code _im} size the same token publishes, cross-checked
     *                   against the one the file itself commits
     */
    public void ofParquet(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long indexTxn,
            long imFileSize
    ) {
        close();
        this.ff = configuration.getFilesFacade();
        this.columnName = columnName;
        this.columnNameTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        this.columnTop = columnTop;
        this.partitionTimestamp = partitionTimestamp;
        this.indexTxn = indexTxn;
        this.decodedRowGroupCount = 0;
        this.decodedRowCount = 0;
        this.imFileSize = imFileSize;
        final int plen = path.size();
        try {
            final LPSZ imFile = ParquetIndexSeal.indexMetaFileName(path, columnName, indexTxn);
            if (IndexMetaFileReader.openAndMapRO(ff, imFile, imReader) == 0) {
                // The token names this pair, so a missing or uncommitted _im is
                // not a "nothing published yet" state the way it is for a
                // writer-side probe: it is the artifact this snapshot was told
                // to read.
                throw CairoException.critical(0)
                        .put("could not read the covering index _im named by the partition metadata [file=")
                        .put(imFile).put(']');
            }
            if (imReader.getPayloadKind() != IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING) {
                // Only arm N is written today. Decoding an arm B payload with
                // arm N's reader is a wrong-answer class, not a crash, so it
                // has to be refused rather than attempted.
                throw CairoException.critical(0)
                        .put("unsupported covering index payload kind [payloadKind=").put(imReader.getPayloadKind())
                        .put(", expected=").put(IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING)
                        .put(", file=").put(imFile).put(']');
            }
            if (imReader.getFileSize() != imFileSize) {
                // The token records the _im size the seal committed. A file
                // committing a different one under the same index_txn is
                // corruption: the artifacts are named by index_txn, so nothing
                // legitimate can rewrite one in place at a different size.
                throw CairoException.critical(0)
                        .put("covering index _im size disagrees with the published token [tokenImFileSize=").put(imFileSize)
                        .put(", imFileSize=").put(imReader.getFileSize())
                        .put(", file=").put(imFile).put(']');
            }
            path.trimTo(plen);
            final LPSZ pidxFile = ParquetIndexSeal.indexParquetFileName(path, columnName, indexTxn);
            // The _im's recorded size, never ff.length(): the file on disk can
            // carry bytes past the committed footer, and mapping those would
            // hand the decoder a footer the seal never committed.
            final long size = imReader.getPidxFileSize();
            if (size <= 0) {
                throw CairoException.critical(0)
                        .put("covering index parquet size is not addressable [pidxFileSize=").put(size)
                        .put(", file=").put(pidxFile).put(']');
            }
            pidxAddr = TableUtils.mapRO(ff, pidxFile, LOG, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            pidxSize = size;
            open = true;
        } catch (Throwable th) {
            close();
            throw th;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void populateCacheForKey(int key) {
        // No-op, and correctly so. The native reader warms a genLookup cache
        // that its cursor would otherwise rebuild by walking the chain; the
        // _im directory answers the same question with a header lookup, so
        // there is nothing to pre-compute. The contract permits this: the
        // method promises the cursor will not be slower afterwards, not that
        // work happened.
    }

    @Override
    public void reloadConditionally() {
        // Nothing can move under this reader. The artifact pair is named by
        // index_txn and is never rewritten in place, so a reseal produces a NEW
        // pair rather than growing this one -- unlike the native chain, whose
        // value file grows and whose generation count advances beneath a bound
        // reader. A moved token is therefore a rebind, not a reload, and
        // TableReader.getIndexReader does it by comparing getIndexTxn() with
        // the partition's published token and calling ofParquet.
    }

    @Override
    public long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k) {
        if (key < 0 || k < 0 || maxValueClamped < minValue) {
            return Numbers.LONG_NULL;
        }
        // The implicit-null prefix comes first in row order and is not in the
        // index at all, so the k-th match may land inside it.
        final long nulls = nullPrefixCount(key, nullMaxValue);
        if (k < nulls) {
            return k;
        }
        long remaining = k - nulls;
        final long range = rowGroupRangeForKey(key);
        if (range == IndexMetaFileReader.KEY_ABSENT) {
            return Numbers.LONG_NULL;
        }
        final int rgLo = Numbers.decodeLowInt(range);
        final int rgHi = Numbers.decodeHighInt(range);
        try (CountingCursor counter = new CountingCursor()) {
            for (int rg = rgLo; rg <= rgHi; rg++) {
                final long rows = imReader.getRowGroupNumRows(rg);
                if (rows <= 0 || isRowGroupPruned(rg, minValue, maxValueClamped)) {
                    continue;
                }
                final long inGroup;
                if (isWholeGroupInRange(rg, minValue, maxValueClamped) && isGroupDedicatedTo(rg, key, rgLo, rgHi)) {
                    inGroup = rows;
                } else {
                    inGroup = counter.countInGroup(rg, key, minValue, maxValueClamped);
                }
                if (remaining < inGroup) {
                    // The k-th match is in THIS group. One decode, wherever the
                    // groups it skipped were countable from metadata.
                    return counter.selectInGroup(rg, key, minValue, maxValueClamped, remaining);
                }
                remaining -= inGroup;
            }
        }
        // k is past the end of the clamped match set. LONG_NULL, never -1:
        // the caller consumes a -1 as an absolute row id.
        return Numbers.LONG_NULL;
    }

    @Override
    public void setFrozen(boolean frozen) {
        this.frozen = frozen;
    }

    @Override
    public void setPinnedTableTxn(long pinnedTableTxn) {
        this.pinnedTableTxn = pinnedTableTxn;
    }
}
