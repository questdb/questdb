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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.BinarySequence;
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
    private static final long PAGE_SIZE = 64 * 1024;

    // aux columns for var-length types (null for fixed-size)
    private final ObjList<MemoryCARW> auxColumns = new ObjList<>();
    private final ObjList<MemoryCARW> columns = new ObjList<>();
    private int columnCount;
    private int[] columnSizes; // byte size per row (0 for var-length)
    private int[] columnTypes;
    private boolean[] isVarLen;
    private GenericRecordMetadata metadata;
    private long rowCount;
    private ObjList<ObjList<String>> symbolTables;
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
                ObjList<String> st = symbolTables.getQuick(i);
                if (st != null) {
                    st.clear();
                }
            }
            symbolTables.clear();
        }
        columnTypes = null;
        columnSizes = null;
        isVarLen = null;
        metadata = null;
        rowCount = 0;
    }

    public void clear() {
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
        if (symbolTables != null) {
            for (int i = 0, n = symbolTables.size(); i < n; i++) {
                ObjList<String> st = symbolTables.getQuick(i);
                if (st != null) {
                    st.clear();
                }
            }
        }
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

    public GenericRecordMetadata getMetadata() {
        return metadata;
    }

    public long getRowCount() {
        return rowCount;
    }

    public ObjList<String> getSymbolTable(int columnIndex) {
        return symbolTables != null ? symbolTables.getQuick(columnIndex) : null;
    }

    public int getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    public long getTimestampAt(long row) {
        if (timestampColumnIndex < 0 || row < 0 || row >= rowCount) {
            return Numbers.LONG_NULL;
        }
        long address = columns.getQuick(timestampColumnIndex).getPageAddress(0);
        return Unsafe.getUnsafe().getLong(address + row * Long.BYTES);
    }

    public void init(GenericRecordMetadata metadata) {
        this.metadata = metadata;
        this.columnCount = metadata.getColumnCount();
        this.columnTypes = new int[columnCount];
        this.columnSizes = new int[columnCount];
        this.isVarLen = new boolean[columnCount];
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
                symbolTables.extendAndSet(i, new ObjList<>());
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

    // TODO(live-view): zero-GC — allocates a new String per non-null symbol (value.toString()) on the refresh hot path,
    //  and does an O(n) linear scan via ObjList.indexOf(). Replace with a CharSequence-keyed open-addressed symbol map
    //  that interns into off-heap storage (see SymbolMapReader / GroupByAllocator patterns).
    public int putSymbol(int col, CharSequence value) {
        ObjList<String> st = symbolTables.getQuick(col);
        if (value == null) {
            putInt(col, -1);
            return -1;
        }
        String s = value.toString();
        int key = st.indexOf(s);
        if (key < 0) {
            key = st.size();
            st.add(s);
        }
        putInt(col, key);
        return key;
    }

    public void incrementRowCount() {
        rowCount++;
    }

    /**
     * Evicts rows whose timestamp falls outside the retention window.
     */
    public void applyRetention(long retentionMicros) {
        if (retentionMicros <= 0 || rowCount == 0 || timestampColumnIndex < 0) {
            return;
        }

        long maxTs = getTimestampAt(rowCount - 1);
        long cutoff = maxTs - retentionMicros;

        long lo = 0;
        long hi = rowCount - 1;
        long tsAddr = columns.getQuick(timestampColumnIndex).getPageAddress(0);
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long ts = Unsafe.getUnsafe().getLong(tsAddr + mid * Long.BYTES);
            if (ts <= cutoff) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (lo > 0) {
            evictRows(lo);
        }
    }

    private void evictRows(long rowsToEvict) {
        long remaining = rowCount - rowsToEvict;
        boolean hasVarLen = false;

        for (int i = 0; i < columnCount; i++) {
            if (isVarLen[i]) {
                hasVarLen = true;
                ColumnTypeDriver driver = ColumnType.getDriver(columnTypes[i]);
                MemoryCARW aux = auxColumns.getQuick(i);
                long auxAddr = aux.getPageAddress(0);
                long srcOffset = driver.getAuxVectorOffset(rowsToEvict);
                // getAuxVectorSize accounts for the trailing N+1 sentinel on STRING/BINARY
                long bytesToMove = driver.getAuxVectorSize(remaining);
                if (bytesToMove > 0 && srcOffset > 0) {
                    Vect.memmove(auxAddr, auxAddr + srcOffset, bytesToMove);
                }
                aux.jumpTo(bytesToMove);
            } else {
                int size = columnSizes[i];
                if (size > 0) {
                    long pageAddr = columns.getQuick(i).getPageAddress(0);
                    long srcOffset = rowsToEvict * size;
                    long bytesToMove = remaining * size;
                    if (bytesToMove > 0) {
                        Vect.memmove(pageAddr, pageAddr + srcOffset, bytesToMove);
                    }
                    columns.getQuick(i).jumpTo(bytesToMove);
                }
            }
        }

        if (hasVarLen) {
            for (int i = 0; i < columnCount; i++) {
                if (!isVarLen[i]) {
                    continue;
                }
                compactVarLenColumn(i, remaining);
            }
        }

        rowCount = remaining;
    }

    /**
     * Compacts a var-length column's data region after {@link #evictRows} shifted its
     * aux entries. Uses the column type driver to locate the surviving data range,
     * memmoves it to position 0, then rewrites the surviving offsets via
     * {@link ColumnTypeDriver#shiftCopyAuxVector} so each driver handles its own
     * descriptor format (simple long for STRING/BINARY, 48-bit packed for VARCHAR,
     * 16-byte descriptor for ARRAY).
     */
    private void compactVarLenColumn(int col, long remaining) {
        ColumnTypeDriver driver = ColumnType.getDriver(columnTypes[col]);
        MemoryCARW data = columns.getQuick(col);
        MemoryCARW aux = auxColumns.getQuick(col);
        if (remaining == 0) {
            data.jumpTo(0);
            aux.jumpTo(0);
            driver.configureAuxMemO3RSS(aux);
            return;
        }

        long dataAddr = data.getPageAddress(0);
        long auxAddr = aux.getPageAddress(0);
        long dataStart = driver.getDataVectorOffset(auxAddr, 0);
        long dataEnd = driver.getDataVectorSizeAt(auxAddr, remaining - 1);
        long dataBytes = dataEnd - dataStart;

        if (dataStart > 0 && dataBytes > 0) {
            Vect.memmove(dataAddr, dataAddr + dataStart, dataBytes);
        }
        data.jumpTo(dataBytes);

        if (dataStart > 0) {
            driver.shiftCopyAuxVector(-dataStart, auxAddr, 0, remaining - 1, auxAddr, aux.size());
        }
    }
}
