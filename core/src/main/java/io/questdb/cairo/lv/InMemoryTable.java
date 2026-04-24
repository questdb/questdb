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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

/**
 * Columnar in-memory store for live view results. Each column is a contiguous
 * native memory region ({@link MemoryCARW}). Variable-length columns use a data+aux pair
 * whose layout matches the on-disk format of the corresponding {@link ColumnTypeDriver},
 * so reads and writes can go through the driver APIs:
 * <ul>
 *     <li>STRING and BINARY use the N+1 aux layout: 8-byte start offsets with a leading
 *     sentinel at aux[0] = 0 and a trailing sentinel at aux[N] = total data size.</li>
 *     <li>VARCHAR uses {@link VarcharTypeDriver}'s 16-byte descriptor (4-byte header,
 *     6-byte inlined prefix, 48-bit data offset packed into bytes 10-15).</li>
 *     <li>ARRAY uses {@link ArrayTypeDriver}'s 16-byte descriptor.</li>
 * </ul>
 */
public class InMemoryTable implements QuietCloseable {
    private static final int SYMBOL_MAP_INITIAL_CAPACITY = 16;
    private static final long PAGE_SIZE = 64 * 1024;
    // Layout of {@link MergeBuffer}'s sort index, duplicated here so
    // {@link #appendFromInSortedOrder} can decode rowIds without a cross-class reach.
    // If MergeBuffer changes its index entry format, update these constants in lockstep.
    private static final int SORT_INDEX_ENTRY_BYTES = 2 * Long.BYTES;
    private static final int SORT_INDEX_ROWID_OFFSET = Long.BYTES;
    private static final long SYMBOL_MAP_INITIAL_BUF_BYTES = 1024;

    // aux columns for var-length types (null for fixed-size)
    private final ObjList<MemoryCARW> auxColumns = new ObjList<>();
    private final ObjList<MemoryCARW> columns = new ObjList<>();
    private int columnCount;
    private int[] columnSizes; // byte size per row (0 for var-length)
    private int[] columnTypes;
    private boolean[] isVarLen;
    private RecordMetadata metadata;
    // Physical first-visible-row index. Advanced by applyRetention to "evict" rows
    // without rewriting the column buffers (ring-buffer style). Reset to 0 by
    // copyFrom (which performs implicit compaction) and by clearRows.
    private long readStart;
    // Physical row count: total rows ever appended into the column buffers, including
    // rows below readStart that have been logically evicted but not yet compacted.
    // Visible rows are [readStart, rowCount); their count is rowCount - readStart.
    private long rowCount;
    // True when this table owns the DirectSymbolMap at the same index and must
    // free it on close. Reset for columns whose dictionary was replaced by a
    // reference via {@link #shareSymbolTablesWith} — those columns share
    // another table's DirectSymbolMap, which owns the native memory.
    private boolean[] ownsSymbolTable;
    private ObjList<DirectSymbolMap> symbolTables;
    private int timestampColumnIndex = -1;

    @Override
    public void close() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            Misc.free(columns.getQuick(i));
        }
        columns.clear();
        for (int i = 0, n = auxColumns.size(); i < n; i++) {
            Misc.free(auxColumns.getQuick(i));
        }
        auxColumns.clear();
        if (symbolTables != null) {
            for (int i = 0, n = symbolTables.size(); i < n; i++) {
                DirectSymbolMap st = symbolTables.getQuick(i);
                if (st != null && ownsSymbolTable[i]) {
                    Misc.free(st);
                }
            }
            symbolTables.clear();
        }
        columnTypes = null;
        columnSizes = null;
        isVarLen = null;
        ownsSymbolTable = null;
        metadata = null;
        rowCount = 0;
    }

    public void clear() {
        clearRows();
        if (symbolTables != null) {
            // Clear shared dictionaries too — a sibling InMemoryTable that shares this
            // column's DirectSymbolMap expects its copy of the (post-swap) state to be
            // reset as a group. Ownership only gates {@link #close}, not clear.
            for (int i = 0, n = symbolTables.size(); i < n; i++) {
                DirectSymbolMap st = symbolTables.getQuick(i);
                if (st != null) {
                    st.clear();
                }
            }
        }
    }

    /**
     * Clears all row data (fixed and var-size columns) and resets the row count,
     * but does not touch symbol dictionaries. Use when symbol dictionaries are shared
     * with another table (see {@link #shareSymbolTablesWith}) and must be kept intact.
     */
    public void clearRows() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            columns.getQuick(i).jumpTo(0);
        }
        for (int i = 0, n = auxColumns.size(); i < n; i++) {
            MemoryCARW aux = auxColumns.getQuick(i);
            if (aux != null) {
                aux.jumpTo(0);
                // re-prime the leading aux[0] = 0 sentinel for STRING/BINARY
                ColumnType.getDriver(columnTypes[i]).configureAuxMemO3RSS(aux);
            }
        }
        rowCount = 0;
        readStart = 0;
    }

    /**
     * Replaces this table's row state with the live region of {@code source}, performing
     * implicit compaction in the process. Only rows in {@code [source.readStart, source.rowCount)}
     * are copied; the destination ends up with {@code readStart = 0} and {@code rowCount =
     * srcVisibleCount}. Variable-length data is sliced to the live range and the aux vector
     * is rewritten with offsets adjusted by {@code -srcDataStart} via the type driver, so
     * the destination layout matches the standard on-disk format. Symbol dictionaries are
     * cloned entry-for-entry so that int keys in the copied data columns still resolve.
     * The {@link RecordMetadata} of the two tables must match — callers are responsible for
     * enforcing that (both buffers in a {@code DoubleBufferedTable} are initialized from
     * the same metadata).
     * <p>
     * This is the periodic-compaction trigger for the ring-buffer eviction model: every
     * incremental refresh that goes through {@code DoubleBufferedTable} executes one
     * {@code copyFrom}, which discards the prefix that {@link #applyRetention} marked
     * evicted and brings the destination back to a packed layout.
     */
    public void copyFrom(InMemoryTable source) {
        long srcReadStart = source.readStart;
        long srcVisibleCount = source.rowCount - srcReadStart;

        for (int i = 0; i < columnCount; i++) {
            MemoryCARW srcData = source.columns.getQuick(i);
            MemoryCARW dstData = columns.getQuick(i);
            dstData.jumpTo(0);

            if (isVarLen[i]) {
                MemoryCARW srcAux = source.auxColumns.getQuick(i);
                MemoryCARW dstAux = auxColumns.getQuick(i);
                ColumnTypeDriver driver = ColumnType.getDriver(columnTypes[i]);

                dstAux.jumpTo(0);
                if (srcVisibleCount == 0) {
                    // Re-prime the leading aux[0] = 0 sentinel for STRING/BINARY (no-op for VARCHAR/ARRAY).
                    driver.configureAuxMemO3RSS(dstAux);
                    continue;
                }

                long srcAuxAddr = srcAux.getPageAddress(0);
                long dataStart = driver.getDataVectorOffset(srcAuxAddr, srcReadStart);
                long dataEnd = driver.getDataVectorSizeAt(srcAuxAddr, srcReadStart + srcVisibleCount - 1);
                long dataBytes = dataEnd - dataStart;

                if (dataBytes > 0) {
                    long dstDataAddr = dstData.appendAddressFor(dataBytes);
                    Vect.memcpy(dstDataAddr, srcData.getPageAddress(0) + dataStart, dataBytes);
                }

                long dstAuxBytes = driver.getAuxVectorSize(srcVisibleCount);
                long dstAuxAddr = dstAux.appendAddressFor(dstAuxBytes);
                // Rewrite aux entries [srcReadStart..srcReadStart+srcVisibleCount-1] into dst with
                // each data offset reduced by dataStart, so the first surviving row's aux entry
                // becomes 0 (matching the STRING/BINARY leading sentinel) and subsequent entries
                // point at the memcpy'd data slice. Per shiftCopyAuxVector's convention,
                // dest = src - shift, so to subtract dataStart we pass +dataStart as the shift.
                driver.shiftCopyAuxVector(
                        dataStart,
                        srcAuxAddr,
                        srcReadStart,
                        srcReadStart + srcVisibleCount - 1,
                        dstAuxAddr,
                        dstAuxBytes
                );
            } else {
                int size = columnSizes[i];
                if (size > 0 && srcVisibleCount > 0) {
                    long bytesToCopy = srcVisibleCount * size;
                    long srcOffset = srcReadStart * size;
                    long dstAddr = dstData.appendAddressFor(bytesToCopy);
                    Vect.memcpy(dstAddr, srcData.getPageAddress(0) + srcOffset, bytesToCopy);
                }
            }
        }

        if (symbolTables != null && source.symbolTables != null) {
            for (int i = 0; i < columnCount; i++) {
                DirectSymbolMap srcSt = source.symbolTables.getQuick(i);
                DirectSymbolMap dstSt = symbolTables.getQuick(i);
                if (srcSt != null && dstSt != null && dstSt != srcSt) {
                    dstSt.copyFrom(srcSt);
                }
            }
        }

        this.readStart = 0;
        this.rowCount = srcVisibleCount;
    }

    /**
     * Returns the aux (offset index) address for a variable-length column.
     */
    public long getAuxColumnAddress(int columnIndex) {
        MemoryCARW aux = auxColumns.getQuick(columnIndex);
        return aux != null ? aux.getPageAddress(0) : 0;
    }

    public long getColumnAddress(int columnIndex) {
        return columns.getQuick(columnIndex).getPageAddress(0);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getColumnSize(int columnIndex) {
        return columnSizes[columnIndex];
    }

    public int getColumnType(int columnIndex) {
        return columnTypes[columnIndex];
    }

    /**
     * Returns the current data size (bytes) of a column's data region.
     */
    public long getDataSize(int columnIndex) {
        return columns.getQuick(columnIndex).getAppendOffset();
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    /**
     * Returns the absolute row count, including rows below {@link #getReadStart()} that
     * have been logically evicted but not yet compacted. Used internally by readers that
     * compute aux/data bounds and by {@link #applyRetention}'s binary search; external
     * consumers want {@link #getRowCount()} (visible count) instead.
     */
    public long getPhysicalRowCount() {
        return rowCount;
    }

    /**
     * Returns the physical index of the first visible row. Reads address physical row
     * {@code virtualRow + readStart}; the bytes below {@code readStart} remain allocated
     * but are no longer reachable through the cursor.
     */
    public long getReadStart() {
        return readStart;
    }

    /**
     * Returns the visible row count, i.e. {@code getPhysicalRowCount() - getReadStart()}.
     * This is the count cursors iterate over and the count
     * {@link io.questdb.griffin.engine.lv.LiveViewRecordCursor#size} reports.
     */
    public long getRowCount() {
        return rowCount - readStart;
    }

    public DirectSymbolMap getSymbolTable(int columnIndex) {
        return symbolTables != null ? symbolTables.getQuick(columnIndex) : null;
    }

    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    /**
     * Returns the timestamp at the given visible row index (0-based, in
     * {@code [0, getRowCount())}). The implementation translates to physical row
     * {@code row + readStart}.
     */
    public long getTimestampAt(long row) {
        if (timestampColumnIndex < 0 || row < 0 || row >= rowCount - readStart) {
            return Numbers.LONG_NULL;
        }
        long address = columns.getQuick(timestampColumnIndex).getPageAddress(0);
        return Unsafe.getUnsafe().getLong(address + (row + readStart) * Long.BYTES);
    }

    public void init(RecordMetadata metadata) {
        this.metadata = metadata;
        this.columnCount = metadata.getColumnCount();
        this.columnTypes = new int[columnCount];
        this.columnSizes = new int[columnCount];
        this.isVarLen = new boolean[columnCount];
        this.ownsSymbolTable = new boolean[columnCount];
        this.timestampColumnIndex = metadata.getTimestampIndex();
        this.symbolTables = new ObjList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            columnTypes[i] = type;
            int tag = ColumnType.tagOf(type);

            boolean varLen = ColumnType.isVarSize(type);
            isVarLen[i] = varLen;

            if (tag == ColumnType.SYMBOL) {
                columnSizes[i] = Integer.BYTES;
                symbolTables.extendAndSet(i, new DirectSymbolMap(
                        SYMBOL_MAP_INITIAL_BUF_BYTES,
                        SYMBOL_MAP_INITIAL_CAPACITY,
                        MemoryTag.NATIVE_DEFAULT
                ));
                ownsSymbolTable[i] = true;
            } else if (varLen) {
                columnSizes[i] = 0;
                symbolTables.extendAndSet(i, null);
            } else {
                columnSizes[i] = ColumnType.sizeOf(type);
                symbolTables.extendAndSet(i, null);
            }

            MemoryCARW dataMem = Vm.getCARWInstance(PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
            columns.extendAndSet(i, dataMem);

            if (varLen) {
                MemoryCARW auxMem = Vm.getCARWInstance(PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                // writes the leading aux[0] = 0 sentinel for STRING/BINARY; no-op for VARCHAR/ARRAY
                ColumnType.getDriver(type).configureAuxMemO3RSS(auxMem);
                auxColumns.extendAndSet(i, auxMem);
            } else {
                auxColumns.extendAndSet(i, null);
            }
        }
        this.rowCount = 0;
    }

    public boolean isVarLen(int columnIndex) {
        return isVarLen[columnIndex];
    }

    /**
     * Replaces this table's per-SYMBOL-column dictionary with references to the
     * other table's, so {@link #putSymbol} in either table interns into the same
     * {@link DirectSymbolMap}. Used by {@link MergeBuffer} to keep partition keys
     * stable across its pending/retained buffer swap: identical string values
     * always resolve to the same int key. This table's original dictionaries for
     * those columns are freed to avoid leaking the native memory they own.
     */
    public void shareSymbolTablesWith(InMemoryTable other) {
        for (int i = 0; i < columnCount; i++) {
            if (ColumnType.tagOf(columnTypes[i]) == ColumnType.SYMBOL) {
                DirectSymbolMap shared = other.symbolTables.getQuick(i);
                DirectSymbolMap current = symbolTables.getQuick(i);
                // {@code shared == current} after a {@link MergeBuffer#compact} swap:
                // the rows/compactScratch pointers flipped, but the underlying
                // dictionary each points at has not moved. Freeing {@code current}
                // and reassigning would drop the only live reference and leave a
                // dangling pointer. Only proceed when we are truly adopting a
                // different dictionary.
                if (shared != null && shared != current) {
                    if (ownsSymbolTable[i]) {
                        Misc.free(current);
                        ownsSymbolTable[i] = false;
                    }
                    symbolTables.setQuick(i, shared);
                }
            }
        }
    }

    public void putBool(int col, boolean value) {
        long addr = columns.getQuick(col).appendAddressFor(Byte.BYTES);
        Unsafe.getUnsafe().putByte(addr, (byte) (value ? 1 : 0));
    }

    public void putByte(int col, byte value) {
        long addr = columns.getQuick(col).appendAddressFor(Byte.BYTES);
        Unsafe.getUnsafe().putByte(addr, value);
    }

    public void putChar(int col, char value) {
        long addr = columns.getQuick(col).appendAddressFor(Character.BYTES);
        Unsafe.getUnsafe().putChar(addr, value);
    }

    public void putDouble(int col, double value) {
        long addr = columns.getQuick(col).appendAddressFor(Double.BYTES);
        Unsafe.getUnsafe().putDouble(addr, value);
    }

    public void putFloat(int col, float value) {
        long addr = columns.getQuick(col).appendAddressFor(Float.BYTES);
        Unsafe.getUnsafe().putFloat(addr, value);
    }

    public void putInt(int col, int value) {
        long addr = columns.getQuick(col).appendAddressFor(Integer.BYTES);
        Unsafe.getUnsafe().putInt(addr, value);
    }

    public void putLong(int col, long value) {
        long addr = columns.getQuick(col).appendAddressFor(Long.BYTES);
        Unsafe.getUnsafe().putLong(addr, value);
    }

    public void putShort(int col, short value) {
        long addr = columns.getQuick(col).appendAddressFor(Short.BYTES);
        Unsafe.getUnsafe().putShort(addr, value);
    }

    public void putArray(int col, @NotNull ArrayView value, int columnType) {
        ArrayTypeDriver.appendValue(auxColumns.getQuick(col), columns.getQuick(col), value);
    }

    /**
     * Appends a BINARY value. Matches {@link io.questdb.cairo.BinaryTypeDriver}'s
     * on-disk format: writes the blob via {@link MemoryCARW#putBin} and appends
     * the post-write data offset (end of the new row = start of the next row) to aux.
     */
    public void putBin(int col, BinarySequence value) {
        MemoryCARW data = columns.getQuick(col);
        MemoryCARW aux = auxColumns.getQuick(col);
        aux.putLong(data.putBin(value));
    }

    /**
     * Appends a STRING value, delegating to {@link StringTypeDriver#appendValue} so
     * the aux vector keeps the N+1 offset layout used by the on-disk format.
     */
    public void putStr(int col, CharSequence value) {
        StringTypeDriver.appendValue(auxColumns.getQuick(col), columns.getQuick(col), value);
    }

    /**
     * Appends a VARCHAR value using {@link VarcharTypeDriver}'s 16-byte aux descriptor,
     * which inlines up to 9 bytes into the aux entry and stores longer values in the data region.
     */
    public void putVarchar(int col, Utf8Sequence value) {
        VarcharTypeDriver.appendValue(auxColumns.getQuick(col), columns.getQuick(col), value);
    }

    public int putSymbol(int col, CharSequence value) {
        if (value == null) {
            putInt(col, -1);
            return -1;
        }
        int key = symbolTables.getQuick(col).intern(value);
        putInt(col, key);
        return key;
    }

    /**
     * Appends {@code to - from} rows from {@code source} into this table in the order
     * the caller's sort index specifies. Each entry of the sort index is a 16-byte
     * {@code (timestamp, physicalRowId)} pair matching {@link MergeBuffer}'s layout;
     * the rowId at offset 8 selects which physical row in {@code source} to copy.
     * <p>
     * Iterates columns outer and rows inner, so the column-type dispatch runs once per
     * column instead of once per {@code (column, row)} pair. Fixed-size columns copy
     * raw bytes per row via {@link Unsafe}; SYMBOL columns fall through the 4-byte
     * branch and copy the int key directly (shared dictionaries via
     * {@link #shareSymbolTablesWith} keep the key stable). Variable-length columns
     * still invoke the type driver per row but without the outer switch dispatch.
     * <p>
     * Preconditions:
     * <ul>
     *     <li>{@code source.readStart == 0} — rowIds in the sort index are physical
     *         and {@link LiveViewRecord#setRow} adds {@code readStart}, so a non-zero
     *         offset would double-translate.</li>
     *     <li>{@code sourceRecord} wraps {@code source}; it is used only to drive the
     *         variable-length getters.</li>
     *     <li>{@code source}'s metadata matches this table's.</li>
     * </ul>
     */
    public void appendFromInSortedOrder(
            InMemoryTable source,
            LiveViewRecord sourceRecord,
            long sortIndexAddr,
            long from,
            long to
    ) {
        long n = to - from;
        if (n <= 0) {
            return;
        }
        long entryBase = sortIndexAddr + from * SORT_INDEX_ENTRY_BYTES;
        for (int col = 0; col < columnCount; col++) {
            if (isVarLen[col]) {
                appendVarLenColumnInSortedOrder(col, sourceRecord, entryBase, n);
            } else {
                appendFixedColumnInSortedOrder(col, source, entryBase, n);
            }
        }
        this.rowCount += n;
    }

    /**
     * Appends a single row from the given {@link Record} into the columnar store
     * and increments the row count. The record must cover every column the table
     * was initialized with.
     */
    public void appendRow(Record record) {
        for (int i = 0; i < columnCount; i++) {
            int type = columnTypes[i];
            switch (ColumnType.tagOf(type)) {
                case ColumnType.INT:
                    putInt(i, record.getInt(i));
                    break;
                case ColumnType.LONG:
                    putLong(i, record.getLong(i));
                    break;
                case ColumnType.TIMESTAMP:
                    putLong(i, record.getTimestamp(i));
                    break;
                case ColumnType.DATE:
                    putLong(i, record.getDate(i));
                    break;
                case ColumnType.DOUBLE:
                    putDouble(i, record.getDouble(i));
                    break;
                case ColumnType.FLOAT:
                    putFloat(i, record.getFloat(i));
                    break;
                case ColumnType.SHORT:
                    putShort(i, record.getShort(i));
                    break;
                case ColumnType.BYTE:
                    putByte(i, record.getByte(i));
                    break;
                case ColumnType.BOOLEAN:
                    putBool(i, record.getBool(i));
                    break;
                case ColumnType.CHAR:
                    putChar(i, record.getChar(i));
                    break;
                case ColumnType.SYMBOL:
                    putSymbol(i, record.getSymA(i));
                    break;
                case ColumnType.STRING:
                    putStr(i, record.getStrA(i));
                    break;
                case ColumnType.VARCHAR:
                    putVarchar(i, record.getVarcharA(i));
                    break;
                case ColumnType.BINARY:
                    putBin(i, record.getBin(i));
                    break;
                default:
                    if (ColumnType.isArray(type)) {
                        putArray(i, record.getArray(i, type), type);
                    } else {
                        throw new UnsupportedOperationException("unsupported column type: " + ColumnType.nameOf(type));
                    }
                    break;
            }
        }
        rowCount++;
    }

    /**
     * Evicts rows whose timestamp falls outside the retention window. The eviction is
     * logical: {@link #readStart} advances to the first physical row with
     * {@code ts > maxTs - retentionMicros}, and the bytes below it stay allocated until
     * the next {@link #copyFrom} (which compacts them out implicitly).
     * <p>
     * Cost: a single binary search over the timestamp column. No memmoves, no per-column
     * work — the eviction touches one scalar field.
     */
    public void applyRetention(long retentionMicros) {
        if (retentionMicros <= 0 || rowCount == readStart || timestampColumnIndex < 0) {
            return;
        }

        long tsAddr = columns.getQuick(timestampColumnIndex).getPageAddress(0);
        long maxTs = Unsafe.getUnsafe().getLong(tsAddr + (rowCount - 1) * Long.BYTES);
        long cutoff = maxTs - retentionMicros;

        // Binary search physical rows in [readStart, rowCount) for the first ts > cutoff.
        long lo = readStart;
        long hi = rowCount - 1;
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long ts = Unsafe.getUnsafe().getLong(tsAddr + mid * Long.BYTES);
            if (ts <= cutoff) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (lo > readStart) {
            readStart = lo;
        }
    }

    private void appendFixedColumnInSortedOrder(int col, InMemoryTable source, long entryBase, long n) {
        int sz = columnSizes[col];
        long srcAddr = source.columns.getQuick(col).getPageAddress(0);
        long dstAddr = columns.getQuick(col).appendAddressFor(n * sz);
        switch (sz) {
            case 1:
                for (long i = 0; i < n; i++) {
                    long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                    Unsafe.getUnsafe().putByte(dstAddr + i, Unsafe.getUnsafe().getByte(srcAddr + rowId));
                }
                break;
            case 2:
                for (long i = 0; i < n; i++) {
                    long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                    Unsafe.getUnsafe().putShort(dstAddr + i * 2, Unsafe.getUnsafe().getShort(srcAddr + rowId * 2));
                }
                break;
            case 4:
                for (long i = 0; i < n; i++) {
                    long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                    Unsafe.getUnsafe().putInt(dstAddr + i * 4, Unsafe.getUnsafe().getInt(srcAddr + rowId * 4));
                }
                break;
            case 8:
                for (long i = 0; i < n; i++) {
                    long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                    Unsafe.getUnsafe().putLong(dstAddr + i * 8, Unsafe.getUnsafe().getLong(srcAddr + rowId * 8));
                }
                break;
            default:
                throw new UnsupportedOperationException("unexpected fixed-size column width: " + sz);
        }
    }

    private void appendVarLenColumnInSortedOrder(int col, LiveViewRecord sourceRecord, long entryBase, long n) {
        int type = columnTypes[col];
        switch (ColumnType.tagOf(type)) {
            case ColumnType.STRING:
                for (long i = 0; i < n; i++) {
                    long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                    sourceRecord.setRow(rowId);
                    putStr(col, sourceRecord.getStrA(col));
                }
                break;
            case ColumnType.VARCHAR:
                for (long i = 0; i < n; i++) {
                    long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                    sourceRecord.setRow(rowId);
                    putVarchar(col, sourceRecord.getVarcharA(col));
                }
                break;
            case ColumnType.BINARY:
                for (long i = 0; i < n; i++) {
                    long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                    sourceRecord.setRow(rowId);
                    putBin(col, sourceRecord.getBin(col));
                }
                break;
            default:
                if (ColumnType.isArray(type)) {
                    for (long i = 0; i < n; i++) {
                        long rowId = Unsafe.getUnsafe().getLong(entryBase + i * SORT_INDEX_ENTRY_BYTES + SORT_INDEX_ROWID_OFFSET);
                        sourceRecord.setRow(rowId);
                        putArray(col, sourceRecord.getArray(col, type), type);
                    }
                } else {
                    throw new UnsupportedOperationException("unsupported var-length type: " + ColumnType.nameOf(type));
                }
                break;
        }
    }
}
