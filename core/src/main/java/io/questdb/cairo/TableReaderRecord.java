/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.Rows;
import io.questdb.std.str.CharSink;

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
    public void getLong256(int col, CharSink sink) {
        long index = getIndex(col);
        if (index < 0) {
            return;
        }
        colA(col).getLong256(index * Long256.BYTES, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return colA(col).getLong256A(index * Long256.BYTES);
    }

    @Override
    public Long256 getLong256B(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return colA(col).getLong256B(index * Long256.BYTES);
    }

    @Override
    public CharSequence getStr(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return colA(col).getStr(colB(col).getLong(index * 8));
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
    public char getChar(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return 0;
        }
        return colA(col).getChar(index * 2);
    }

    @Override
    public CharSequence getStrB(int col) {
        long index = getIndex(col);
        if (index < 0) {
            return null;
        }
        return colA(col).getStr2(colB(col).getLong(index * 8));
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

    public void setRecordIndex(long recordIndex) {
        this.recordIndex = recordIndex;
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

    private ReadOnlyColumn colA(int col) {
        return reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, col));
    }

    private ReadOnlyColumn colB(int col) {
        return reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, col) + 1);
    }

    private long getIndex(int col) {
        assert col > -1 && col < reader.getColumnCount() : "Column index out of bounds: " + col + " >= " + reader.getColumnCount();
        final long top = reader.getColumnTop(columnBase, col);
        return top > 0L ? recordIndex - top : recordIndex;
    }
}
