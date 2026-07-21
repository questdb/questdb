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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Vect;

/**
 * A day-scoped, off-heap, per-column row accumulator used to batch high-frequency WAL commits for a
 * composite-partitioned table (composite-partitioning Plan #5, Task 2 -- the isolated storage
 * component; see {@code docs/superpowers/plans/2026-07-20-composite-deferred-5-wal-lag-batching.md}).
 * <p>
 * A plain table's WAL lag appends small commits directly into the last partition's on-disk column
 * files ({@code TableWriter.columns}), keyed by a single day. A composite table has no such single
 * target -- it fans rows out to N per-cell segments -- so its lag needs a separate, cell-agnostic RAM
 * holding area that simply accumulates the already-remapped WAL row values (in table-column order)
 * across several WAL transactions, until a flush routes them through the existing per-row cell
 * dispatch ({@code TableWriter#processO3BlockComposite} -&gt; {@code resolveRowCellKey} -&gt;
 * {@code dispatchCompositeCellRange}).
 * <p>
 * THIS CLASS IS INTENTIONALLY NOT WIRED INTO THE COMMIT PATH. It is a self-contained, unit-tested
 * component: constructing, appending to, and closing an instance has zero effect on any
 * {@code TableWriter} state or on-disk data. A later task wires it into {@code processWalCommit}
 * behind a config flag.
 * <p>
 * Memory layout mirrors the writer's existing O3 staging idiom: one growable, contiguous
 * {@link MemoryCARW} region per column (the same concrete type backing {@code TableWriter.o3Columns}
 * / {@code o3MemColumns1}, obtained the same way via {@link Vm#getCARWInstance(long, int, int)}), so a
 * later flush can read a column's valid bytes directly off {@link #getColumnAddress(int)} /
 * {@link #getColumnMemory(int)} with no copy, the same way {@code cthAppendWalColumnToLastPartition}
 * reads {@code o3Columns} today. {@link MemoryCARW} already grows (via realloc, doubling page count)
 * and preserves previously-written bytes on growth, so this class does not re-implement growth; it
 * only tracks, per column, how many rows have been appended and copies new ranges in at the current
 * append offset.
 * <p>
 * <b>Scope -- fixed-width columns only.</b> This buffer supports only fixed-width column types
 * (TIMESTAMP, LONG, DOUBLE, INT, SHORT, BYTE, BOOLEAN, FLOAT, CHAR, DATE, the GEO* family, LONG256,
 * UUID, LONG128, and SYMBOL -- which, like the rest of the O3 staging layer, is stored as a plain
 * 4-byte int key, never as its string value). Variable-length columns (VARCHAR, STRING, BINARY, and
 * ARRAY of any shape -- i.e. every type {@link ColumnType#isVarSize(int)} reports true for, since
 * those are exactly the types with a secondary/aux index vector this buffer does not model) are OUT
 * OF SCOPE: the constructor rejects any such column type immediately and loudly
 * ({@link UnsupportedOperationException}), rather than silently mishandling it later. The caller (the
 * commit-path integration, Task 3) is responsible for checking a composite table's column list up
 * front and routing any table with a var-len column to the existing safe full-commit path instead of
 * ever constructing this buffer over it.
 * <p>
 * Not thread-safe; a single writer thread is assumed, matching every other {@code TableWriter}-owned
 * O3 staging structure.
 */
public class CompositeWalLagBuffer implements QuietCloseable {

    /**
     * Initial per-column region size, in bytes, before any growth. Matches the precedent set by other
     * small O3-adjacent scratch regions in {@code TableWriter}/{@code O3ParquetMergeContext} (e.g.
     * {@code REBASE_AUX_ARENA_PAGE_SIZE}) -- small enough not to over-commit for a lag meant to hold a
     * handful of WAL transactions, doubling on demand via {@link MemoryCARW}'s own growth.
     */
    private static final long DEFAULT_PAGE_SIZE = 16 * 1024;

    private final int columnCount;
    private final ObjList<MemoryCARW> columns;
    private final IntList columnTypes;
    private final long pageSize;
    private long rowCount = 0;

    /**
     * @param columnTypes the table's column types, indexed 0..columnCount-1 (the same dense,
     *                    zero-based column-index convention used everywhere else in {@code
     *                    TableWriter}, i.e. NOT the doubled primary/secondary {@code o3Columns}
     *                    convention -- see {@link #append(ReadOnlyObjList, long, long)}). Copied
     *                    defensively; the caller's list may be mutated/reused afterward.
     * @throws UnsupportedOperationException if any column type is variable-length
     *                                        ({@link ColumnType#isVarSize(int)}).
     */
    public CompositeWalLagBuffer(IntList columnTypes) {
        this(columnTypes, DEFAULT_PAGE_SIZE);
    }

    /**
     * As {@link #CompositeWalLagBuffer(IntList)}, but with an explicit initial per-column region size.
     * Exposed primarily so tests can force growth/reallocation deterministically with a small page
     * size; production callers should use the single-arg constructor.
     */
    public CompositeWalLagBuffer(IntList columnTypes, long initialPageSize) {
        this.columnCount = columnTypes.size();
        this.columnTypes = new IntList(columnTypes);
        for (int i = 0; i < columnCount; i++) {
            assertFixedWidth(this.columnTypes.getQuick(i), i);
        }
        this.columns = new ObjList<>(columnCount);
        this.pageSize = initialPageSize;
    }

    /**
     * Appends one contiguous row range, for every column, from {@code srcColumns} into this buffer at
     * the current append offset, then advances {@link #getRowCount()} by {@code srcRowHi - srcRowLo}.
     * <p>
     * {@code srcColumns} must be dense and indexed by plain column index (0..columnCount-1) -- i.e.
     * ONE entry per column, already dereferenced to that column's data/primary memory. This is
     * deliberately NOT the doubled primary/secondary indexing convention {@code TableWriter.o3Columns}
     * uses ({@code getPrimaryColumnIndex(i) == i * 2}); since every column here is fixed-width there is
     * no secondary/aux slot to carry. A caller holding a real {@code o3Columns}-shaped list builds this
     * dense view with one pass, e.g. {@code srcColumns.getQuick(TableWriter.getPrimaryColumnIndex(i))}
     * per column index {@code i}.
     * <p>
     * All source values are read as raw bytes (a plain {@code memcpy} sized by
     * {@link ColumnType#pow2SizeOf(int)}); this is valid for every fixed-width type this buffer
     * accepts, including SYMBOL, which is a plain 4-byte int key at this layer, identical to how
     * {@code TableWriter} itself treats it in {@code o3Columns}.
     *
     * @param srcColumns dense, per-column source memory, size == {@link #getColumnCount()}
     * @param srcRowLo   first row to copy, inclusive
     * @param srcRowHi   last row to copy, exclusive
     */
    public void append(ReadOnlyObjList<? extends MemoryCR> srcColumns, long srcRowLo, long srcRowHi) {
        if (srcRowHi < srcRowLo) {
            throw new IllegalArgumentException("srcRowHi (" + srcRowHi + ") < srcRowLo (" + srcRowLo + ')');
        }
        if (srcColumns.size() != columnCount) {
            throw new IllegalArgumentException(
                    "srcColumns size (" + srcColumns.size() + ") != column count (" + columnCount + ')'
            );
        }
        long rows = srcRowHi - srcRowLo;
        if (rows == 0) {
            return;
        }
        for (int i = 0; i < columnCount; i++) {
            final int shl = ColumnType.pow2SizeOf(columnTypes.getQuick(i));
            final MemoryCARW dst = ensureColumn(i);
            final MemoryCR src = srcColumns.getQuick(i);
            final long dstOffset = rowCount << shl;
            final long lenBytes = rows << shl;
            // jumpTo grows (and preserves existing bytes via realloc) if needed, then we re-fetch the
            // address fresh -- the underlying region's base address is NOT stable across growth.
            dst.jumpTo(dstOffset + lenBytes);
            final long dstAddr = dst.addressOf(dstOffset);
            final long srcAddr = src.addressOf(srcRowLo << shl);
            Vect.memcpy(dstAddr, srcAddr, lenBytes);
        }
        rowCount += rows;
    }

    /**
     * Resets the row count to zero and rewinds every column's append cursor, WITHOUT releasing or
     * shrinking any already-grown native region -- capacity is retained for the next accumulate cycle
     * (deliberately not {@code MemoryCARW.truncate()}, which reallocates down to a single page).
     */
    public void clear() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            MemoryCARW mem = columns.getQuiet(i);
            if (mem != null) {
                mem.jumpTo(0);
            }
        }
        rowCount = 0;
    }

    /**
     * Releases every column's native region. Idempotent (safe to call more than once; safe to call on
     * a buffer that never had any column allocated).
     */
    @Override
    public void close() {
        Misc.freeObjListAndKeepObjects(columns);
        rowCount = 0;
    }

    /**
     * @param columnIndex 0-based column index
     * @return the base address of column {@code columnIndex}'s valid data (row 0), or 0 if the column
     * has never been appended to. Valid for {@link #getRowCount()} rows of
     * {@code ColumnType.sizeOf(getColumnType(columnIndex))} bytes each, until the next {@link #append}
     * / {@link #clear} / {@link #close} call -- like any {@code MemoryCARW}, the region may move on
     * growth, so callers must re-fetch after mutating, never cache across an {@link #append} call.
     */
    public long getColumnAddress(int columnIndex) {
        checkColumnIndex(columnIndex);
        MemoryCARW mem = columns.getQuiet(columnIndex);
        return mem != null ? mem.addressOf(0) : 0;
    }

    public int getColumnCount() {
        return columnCount;
    }

    /**
     * @param columnIndex 0-based column index
     * @return the underlying {@link MemoryCARW} region backing column {@code columnIndex}, or null if
     * the column has never been appended to. Exposed so a flush can hand this directly to code that
     * already consumes {@code MemoryCR}-typed handles (as {@code processO3BlockComposite}'s O3 merge
     * machinery does for {@code o3Columns}) instead of going through the raw address/size accessors.
     */
    public MemoryCARW getColumnMemory(int columnIndex) {
        checkColumnIndex(columnIndex);
        return columns.getQuiet(columnIndex);
    }

    /**
     * @param columnIndex 0-based column index
     * @return the total valid byte length of column {@code columnIndex}'s data, i.e.
     * {@code getRowCount() * ColumnType.sizeOf(getColumnType(columnIndex))}.
     */
    public long getColumnSize(int columnIndex) {
        checkColumnIndex(columnIndex);
        return rowCount << ColumnType.pow2SizeOf(columnTypes.getQuick(columnIndex));
    }

    public int getColumnType(int columnIndex) {
        checkColumnIndex(columnIndex);
        return columnTypes.getQuick(columnIndex);
    }

    public long getRowCount() {
        return rowCount;
    }

    private static void assertFixedWidth(int columnType, int columnIndex) {
        if (ColumnType.isVarSize(columnType)) {
            throw new UnsupportedOperationException(
                    "CompositeWalLagBuffer supports fixed-width columns only; column " + columnIndex +
                            " has variable-length type " + ColumnType.nameOf(columnType) +
                            " (VARCHAR/STRING/BINARY/ARRAY are out of scope for this buffer -- a composite " +
                            "table with such a column must be routed through the full-commit path instead)"
            );
        }
    }

    private void checkColumnIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columnCount) {
            throw new IllegalArgumentException("column index out of range [0, " + columnCount + "): " + columnIndex);
        }
    }

    private MemoryCARW ensureColumn(int columnIndex) {
        MemoryCARW mem = columns.getQuiet(columnIndex);
        if (mem == null) {
            // Defense in depth: the constructor already rejects every column up front, so this can
            // only re-trip if the immutable columnTypes list were somehow inconsistent.
            assertFixedWidth(columnTypes.getQuick(columnIndex), columnIndex);
            mem = Vm.getCARWInstance(pageSize, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
            columns.extendAndSet(columnIndex, mem);
        }
        return mem;
    }
}
