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

package io.questdb.griffin.engine.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.lv.InMemoryTable;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectByteSequenceView;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;

/**
 * Record implementation that reads column values from the InMemoryTable's
 * native memory columns. Fixed-size columns use direct offset arithmetic.
 * Variable-length columns (STRING, VARCHAR) use a data+aux layout.
 */
public class LiveViewRecord implements Record {
    private final BorrowedArray arrayView = new BorrowedArray();
    private final DirectByteSequenceView binView = new DirectByteSequenceView();
    private final DirectString csViewA = new DirectString();
    private final DirectString csViewB = new DirectString();
    private final InMemoryTable table;
    private final DirectUtf8String utf8ViewA = new DirectUtf8String();
    private final DirectUtf8String utf8ViewB = new DirectUtf8String();
    private long row;

    public LiveViewRecord(InMemoryTable table) {
        this.table = table;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long auxSize = ColumnType.getDriver(columnType).auxRowsToBytes(table.getRowCount());
        long dataSize = table.getDataSize(col);
        arrayView.of(columnType, auxAddr, auxAddr + auxSize, dataAddr, dataAddr + dataSize, row);
        return arrayView;
    }

    @Override
    public BinarySequence getBin(int col) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = Unsafe.getUnsafe().getLong(auxAddr + row * Long.BYTES);
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
        long offset = Unsafe.getUnsafe().getLong(auxAddr + row * Long.BYTES);
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
        long offset = Unsafe.getUnsafe().getLong(auxAddr + row * Long.BYTES);
        return Unsafe.getUnsafe().getInt(dataAddr + offset);
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
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = Unsafe.getUnsafe().getLong(auxAddr + row * Long.BYTES);
        return Unsafe.getUnsafe().getInt(dataAddr + offset);
    }

    @Override
    public long getRowId() {
        return row;
    }

    public void setRow(long row) {
        this.row = row;
    }

    private CharSequence getStr0(int col, DirectString view) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = Unsafe.getUnsafe().getLong(auxAddr + row * Long.BYTES);
        int len = Unsafe.getUnsafe().getInt(dataAddr + offset);
        if (len == TableUtils.NULL_LEN) {
            return null;
        }
        return view.of(dataAddr + offset + Integer.BYTES, len);
    }

    private Utf8Sequence getVarchar0(int col, DirectUtf8String view) {
        long auxAddr = table.getAuxColumnAddress(col);
        long dataAddr = table.getColumnAddress(col);
        long offset = Unsafe.getUnsafe().getLong(auxAddr + row * Long.BYTES);
        int size = Unsafe.getUnsafe().getInt(dataAddr + offset);
        if (size == TableUtils.NULL_LEN) {
            return null;
        }
        return view.of(dataAddr + offset + Integer.BYTES, dataAddr + offset + Integer.BYTES + size);
    }
}
