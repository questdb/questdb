package io.questdb.griffin.engine.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.InMemoryTable;
import io.questdb.cairo.sql.Record;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * Record implementation that reads column values from the InMemoryTable's
 * native memory columns. Each column is a contiguous MemoryCARW region.
 */
public class LiveViewRecord implements Record {
    private final InMemoryTable table;
    private long row;

    public LiveViewRecord(InMemoryTable table) {
        this.table = table;
    }

    @Override
    public boolean getBool(int col) {
        return Unsafe.getUnsafe().getByte(table.getColumnAddress(col) + row) != 0;
    }

    @Override
    public byte getByte(int col) {
        return Unsafe.getUnsafe().getByte(table.getColumnAddress(col) + row);
    }

    @Override
    public char getChar(int col) {
        return Unsafe.getUnsafe().getChar(table.getColumnAddress(col) + row * Character.BYTES);
    }

    @Override
    public long getDate(int col) {
        return getLong(col);
    }

    @Override
    public double getDouble(int col) {
        return Unsafe.getUnsafe().getDouble(table.getColumnAddress(col) + row * Double.BYTES);
    }

    @Override
    public float getFloat(int col) {
        return Unsafe.getUnsafe().getFloat(table.getColumnAddress(col) + row * Float.BYTES);
    }

    @Override
    public int getInt(int col) {
        return Unsafe.getUnsafe().getInt(table.getColumnAddress(col) + row * Integer.BYTES);
    }

    @Override
    public long getLong(int col) {
        return Unsafe.getUnsafe().getLong(table.getColumnAddress(col) + row * Long.BYTES);
    }

    @Override
    public short getShort(int col) {
        return Unsafe.getUnsafe().getShort(table.getColumnAddress(col) + row * Short.BYTES);
    }

    @Override
    public CharSequence getSymA(int col) {
        int key = getInt(col);
        if (key < 0) {
            return null;
        }
        ObjList<String> st = table.getSymbolTable(col);
        return st != null && key < st.size() ? st.getQuick(key) : null;
    }

    @Override
    public CharSequence getSymB(int col) {
        return getSymA(col);
    }

    @Override
    public long getTimestamp(int col) {
        return getLong(col);
    }

    public void setRow(long row) {
        this.row = row;
    }
}
