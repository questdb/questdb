package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.std.BinarySequence;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8Sequence;

/**
 * Columnar in-memory store for live view results. Each column is a contiguous
 * native memory region ({@link MemoryCARW}). Variable-length columns (STRING,
 * VARCHAR) use a data+aux pair: aux stores per-row offsets into the data region.
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

    /**
     * Appends an ARRAY value using the native column format (16-byte aux entries).
     */
    public void putArray(int col, ArrayView value, int columnType) {
        MemoryCARW data = columns.getQuick(col);
        MemoryCARW aux = auxColumns.getQuick(col);
        if (value == null || value.isNull()) {
            ArrayTypeDriver.appendValue(aux, data, value);
        } else {
            ArrayTypeDriver.appendValue(aux, data, value);
        }
    }

    /**
     * Appends a BINARY value. Writes the blob to the data column and
     * records the data offset in the aux column.
     */
    public void putBin(int col, BinarySequence value) {
        MemoryCARW data = columns.getQuick(col);
        MemoryCARW aux = auxColumns.getQuick(col);
        long dataOffset = data.getAppendOffset();
        long auxAddr = aux.appendAddressFor(Long.BYTES);
        Unsafe.getUnsafe().putLong(auxAddr, dataOffset);
        data.putBin(value);
    }

    /**
     * Appends a STRING value. Writes the string to the data column and
     * records the data offset in the aux column.
     */
    public void putStr(int col, CharSequence value) {
        MemoryCARW data = columns.getQuick(col);
        MemoryCARW aux = auxColumns.getQuick(col);
        long dataOffset = data.getAppendOffset();
        // write offset to aux
        long auxAddr = aux.appendAddressFor(Long.BYTES);
        Unsafe.getUnsafe().putLong(auxAddr, dataOffset);
        // write string to data
        data.putStr(value);
    }

    /**
     * Appends a VARCHAR value. Uses the same data+aux layout as STRING.
     */
    public void putVarchar(int col, Utf8Sequence value) {
        MemoryCARW data = columns.getQuick(col);
        MemoryCARW aux = auxColumns.getQuick(col);
        long dataOffset = data.getAppendOffset();
        long auxAddr = aux.appendAddressFor(Long.BYTES);
        Unsafe.getUnsafe().putLong(auxAddr, dataOffset);
        // store as length-prefixed UTF-8
        if (value != null) {
            int size = value.size();
            long addr = data.appendAddressFor(Integer.BYTES + size);
            Unsafe.getUnsafe().putInt(addr, size);
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(addr + Integer.BYTES + i, value.byteAt(i));
            }
        } else {
            long addr = data.appendAddressFor(Integer.BYTES);
            Unsafe.getUnsafe().putInt(addr, TableUtils.NULL_LEN);
        }
    }

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

        // evict fixed-size and aux columns
        for (int i = 0; i < columnCount; i++) {
            if (isVarLen[i]) {
                hasVarLen = true;
                // shift aux entries
                long auxEntrySize = ColumnType.getDriver(columnTypes[i]).auxRowsToBytes(1);
                MemoryCARW aux = auxColumns.getQuick(i);
                long auxAddr = aux.getPageAddress(0);
                long srcOffset = rowsToEvict * auxEntrySize;
                long bytesToMove = remaining * auxEntrySize;
                if (bytesToMove > 0) {
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

        // for var-length columns, compact the data region and adjust offsets
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
     * Compacts a var-length column's data region after aux entries have been
     * shifted. Uses the column's type driver to determine data boundaries,
     * then memcpy's the surviving data range to position 0 and adjusts
     * all aux offsets by the same delta.
     */
    private void compactVarLenColumn(int col, long remaining) {
        if (remaining == 0) {
            columns.getQuick(col).jumpTo(0);
            return;
        }
        MemoryCARW data = columns.getQuick(col);
        MemoryCARW aux = auxColumns.getQuick(col);
        long dataAddr = data.getPageAddress(0);
        long auxAddr = aux.getPageAddress(0);

        var driver = ColumnType.getDriver(columnTypes[col]);
        // first surviving row's data offset = start of data to keep
        long dataStart = driver.getDataVectorOffset(auxAddr, 0);
        // last surviving row's data end
        long dataEnd = driver.getDataVectorSizeAt(auxAddr, remaining - 1);
        long dataBytes = dataEnd - dataStart;

        if (dataStart > 0 && dataBytes > 0) {
            Vect.memmove(dataAddr, dataAddr + dataStart, dataBytes);
        }
        data.jumpTo(dataBytes);

        // adjust all aux offsets by -dataStart
        if (dataStart > 0) {
            long auxEntrySize = driver.auxRowsToBytes(1);
            for (long row = 0; row < remaining; row++) {
                long auxEntryAddr = auxAddr + row * auxEntrySize;
                long oldOffset = Unsafe.getUnsafe().getLong(auxEntryAddr);
                Unsafe.getUnsafe().putLong(auxEntryAddr, oldOffset - dataStart);
            }
        }
    }
}
