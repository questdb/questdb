package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Columnar in-memory store for live view results. Each column is a contiguous
 * native memory region ({@link MemoryCARW}). Only fixed-size column types
 * are supported in V1.
 */
public class InMemoryTable implements QuietCloseable {
    private static final long PAGE_SIZE = 64 * 1024; // 64 KB initial page

    private final ObjList<MemoryCARW> columns = new ObjList<>();
    private int columnCount;
    private int[] columnSizes; // byte size of each column type
    private int[] columnTypes;
    private GenericRecordMetadata metadata;
    private long rowCount;
    // per-column symbol tables (null for non-symbol columns)
    private ObjList<ObjList<String>> symbolTables;
    private int timestampColumnIndex = -1;

    @Override
    public void close() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            Misc.free(columns.getQuick(i));
        }
        columns.clear();
        rowCount = 0;
    }

    public void clear() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            columns.getQuick(i).jumpTo(0);
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
        this.timestampColumnIndex = metadata.getTimestampIndex();
        this.symbolTables = new ObjList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            int type = metadata.getColumnType(i);
            columnTypes[i] = type;
            int size = ColumnType.sizeOf(type);
            if (ColumnType.tagOf(type) == ColumnType.SYMBOL) {
                // store symbols as INT keys
                size = Integer.BYTES;
                symbolTables.extendAndSet(i, new ObjList<>());
            } else {
                symbolTables.extendAndSet(i, null);
            }
            columnSizes[i] = size;

            MemoryCARW mem = Vm.getCARWInstance(PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
            columns.extendAndSet(i, mem);
        }
        this.rowCount = 0;
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

    public void putByte(int col, byte value) {
        long addr = columns.getQuick(col).appendAddressFor(Byte.BYTES);
        Unsafe.getUnsafe().putByte(addr, value);
    }

    public void putBool(int col, boolean value) {
        long addr = columns.getQuick(col).appendAddressFor(Byte.BYTES);
        Unsafe.getUnsafe().putByte(addr, (byte) (value ? 1 : 0));
    }

    public void putChar(int col, char value) {
        long addr = columns.getQuick(col).appendAddressFor(Character.BYTES);
        Unsafe.getUnsafe().putChar(addr, value);
    }

    /**
     * Interns a symbol value and stores the int key.
     *
     * @return the interned key
     */
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
     * Retention is by timestamp range: rows older than (maxTimestamp - retentionMicros)
     * are removed.
     */
    public void applyRetention(long retentionMicros) {
        if (retentionMicros <= 0 || rowCount == 0 || timestampColumnIndex < 0) {
            return;
        }

        long maxTs = getTimestampAt(rowCount - 1);
        long cutoff = maxTs - retentionMicros;

        // binary search for the first row with ts > cutoff
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
        // lo = first row to keep
        if (lo > 0) {
            evictRows(lo);
        }
    }

    private void evictRows(long rowsToEvict) {
        long remaining = rowCount - rowsToEvict;
        for (int i = 0; i < columnCount; i++) {
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
        rowCount = remaining;
    }
}
