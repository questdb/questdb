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

import io.questdb.cairo.BinaryTypeDriver;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectByteSequenceView;
import io.questdb.std.DirectSymbolMap;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;

/**
 * Record implementation that reads column values from the InMemoryTable's
 * native memory columns. Fixed-size columns use direct offset arithmetic.
 * Variable-length columns (STRING, VARCHAR) use a data+aux layout.
 * <p>
 * The {@link #row} field stores the <em>physical</em> row index into the table's
 * column buffers. {@link #setRow} accepts a <em>virtual</em> row (0-based, matching
 * the cursor's iteration index) and translates it to physical via the table's
 * {@code readStart}; {@link #getRowId} reverses the translation. This keeps the
 * external rowId space dense ({@code [0, getRowCount())}) while internal reads use
 * the physical address directly with no per-getter arithmetic.
 */
public class LiveViewRecord implements Record {
    private final BorrowedArray arrayView = new BorrowedArray();
    private final DirectByteSequenceView binView = new DirectByteSequenceView();
    private final DirectString csViewA = new DirectString();
    private final DirectString csViewB = new DirectString();
    private final DirectString symViewA = new DirectString();
    private final DirectString symViewB = new DirectString();
    private final Utf8SplitString utf8ViewA = new Utf8SplitString();
    private final Utf8SplitString utf8ViewB = new Utf8SplitString();
    private long row;
    private InMemoryTable table;

    public LiveViewRecord(InMemoryTable table) {
        this.table = table;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        // Bounds must encompass aux entries for every appended row (including any below
        // readStart that have not been compacted yet), so use the physical row count.
        long auxSize = ColumnType.getDriver(columnType).auxRowsToBytes(table.getPhysicalRowCount());
        long dataSize = table.getDataSize(col);
        arrayView.of(columnType, auxAddr, auxAddr + auxSize, dataAddr, dataAddr + dataSize, row);
        return arrayView;
    }

    @Override
    public BinarySequence getBin(int col) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = BinaryTypeDriver.INSTANCE.getDataVectorOffset(auxAddr, row);
        long len = Unsafe.getUnsafe().getLong(dataAddr + offset);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        return binView.of(dataAddr + offset + Long.BYTES, len);
    }

    @Override
    public long getBinLen(int col) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = BinaryTypeDriver.INSTANCE.getDataVectorOffset(auxAddr, row);
        return Unsafe.getUnsafe().getLong(dataAddr + offset);
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
    public CharSequence getStrA(int col) {
        return getStr0(col, csViewA);
    }

    @Override
    public CharSequence getStrB(int col) {
        return getStr0(col, csViewB);
    }

    @Override
    public int getStrLen(int col) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = StringTypeDriver.INSTANCE.getDataVectorOffset(auxAddr, row);
        return Unsafe.getUnsafe().getInt(dataAddr + offset);
    }

    @Override
    public CharSequence getSymA(int col) {
        int key = getInt(col);
        if (key < 0) {
            return null;
        }
        DirectSymbolMap st = table.getSymbolTable(col);
        return st != null ? st.valueOf(key, symViewA) : null;
    }

    @Override
    public CharSequence getSymB(int col) {
        int key = getInt(col);
        if (key < 0) {
            return null;
        }
        DirectSymbolMap st = table.getSymbolTable(col);
        return st != null ? st.valueOf(key, symViewB) : null;
    }

    @Override
    public long getTimestamp(int col) {
        return getLong(col);
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return getVarchar0(col, utf8ViewA);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return getVarchar0(col, utf8ViewB);
    }

    @Override
    public int getVarcharSize(int col) {
        return VarcharTypeDriver.getValueSize(table.getAuxColumnAddress(col), row);
    }

    @Override
    public long getRowId() {
        // External rowId is virtual (0-based); physical row stored internally.
        return table != null ? row - table.getReadStart() : row;
    }

    public void setRow(long row) {
        // Translate virtual row to physical for direct addressing in the column getters.
        this.row = row + (table != null ? table.getReadStart() : 0);
    }

    public void setTable(InMemoryTable table) {
        this.table = table;
    }

    private CharSequence getStr0(int col, DirectString view) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = StringTypeDriver.INSTANCE.getDataVectorOffset(auxAddr, row);
        int len = Unsafe.getUnsafe().getInt(dataAddr + offset);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        return view.of(dataAddr + offset + Integer.BYTES, len);
    }

    private Utf8Sequence getVarchar0(int col, Utf8SplitString view) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        // Same bound rationale as getArray: physical row count covers all aux entries.
        long auxLim = auxAddr + VarcharTypeDriver.INSTANCE.getAuxVectorSize(table.getPhysicalRowCount());
        long dataLim = dataAddr + table.getDataSize(col);
        return VarcharTypeDriver.getSplitValue(auxAddr, auxLim, dataAddr, dataLim, row, view);
    }
}
