/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.Numbers;
import com.questdb.std.Rows;

public class TableReaderRecord implements Record {

    private int columnBase;
    private long recordIndex = 0;
    private TableReader reader;

    @Override
    public BinarySequence getBin(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return colA(col).getBin(colB(col).getLong(index * 8));
    }

    @Override
    public long getBinLen(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return TableUtils.NULL_LEN;
        }
        return colA(col).getBinLen(colB(col).getLong(index * 8));
    }

    @Override
    public boolean getBool(int col) {
        long index = getIndex(col);
        return index >= 0 && colA(col).getBool(index);
    }

    @Override
    public byte getByte(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return 0;
        }
        return colA(col).getByte(index);
    }

    @Override
    public double getDouble(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return Double.NaN;
        }
        return colA(col).getDouble(index * 8);
    }

    @Override
    public float getFloat(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return Float.NaN;
        }
        return colA(col).getFloat(index * 4);
    }

    @Override
    public CharSequence getFlyweightStr(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return colA(col).getStr(colB(col).getLong(index * 8));
    }

    @Override
    public CharSequence getFlyweightStrB(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return colA(col).getStr2(colB(col).getLong(index * 8));
    }

    @Override
    public int getInt(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return Numbers.INT_NaN;
        }
        return colA(col).getInt(index * 4);
    }

    @Override
    public long getLong(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return Numbers.LONG_NaN;
        }
        return colA(col).getLong(index * 8);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(reader.getPartitionIndex(columnBase), recordIndex);
    }

    @Override
    public short getShort(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return 0;
        }
        return colA(col).getShort(index * 2);
    }

    @Override
    public int getStrLen(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return TableUtils.NULL_LEN;
        }
        return colA(col).getStrLen(colB(col).getLong(index * 8));
    }

    @Override
    public CharSequence getSym(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return reader.getSymbolMapReader(col).value(colA(col).getInt(index * 4));
    }

    public long getRecordIndex() {
        return recordIndex;
    }

    public void of(TableReader reader) {
        this.reader = reader;
    }

    public void incrementRecordIndex() {
        recordIndex++;
    }

    public void jumpTo(int partitionIndex, long recordIndex) {
        this.columnBase = reader.getColumnBase(partitionIndex);
        this.recordIndex = recordIndex;
    }

    public void setRecordIndex(long recordIndex) {
        this.recordIndex = recordIndex;
    }

    private ReadOnlyColumn colA(int col) {
        return reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, col));
    }

    private ReadOnlyColumn colB(int col) {
        return reader.getColumn(TableReader.getSecondaryColumnIndex(columnBase, col));
    }

    private long getIndex(int col) {
        assert col > -1 && col < reader.getColumnCount() : "Column index out of bounds: " + col + " >= " + reader.getColumnCount();
        final long top = reader.getColumnTop(columnBase, col);
        return top > 0L ? recordIndex - top : recordIndex;
    }
}
