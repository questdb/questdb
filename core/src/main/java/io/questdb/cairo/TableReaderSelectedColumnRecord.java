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
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class TableReaderSelectedColumnRecord implements Record {

    private final IntList columnIndexes;
    private int columnBase;
    private long recordIndex = 0;
    private TableReader reader;

    public TableReaderSelectedColumnRecord(IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return null;
        }
        return colA(col).getBin(colB(col).getLong(recordIndex * 8));
    }

    @Override
    public long getBinLen(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return TableUtils.NULL_LEN;
        }
        return colA(col).getBinLen(colB(col).getLong(recordIndex * 8));
    }

    @Override
    public boolean getBool(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        return recordIndex > -1 && colA(col).getBool(recordIndex);
    }

    @Override
    public byte getByte(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return 0;
        }
        return colA(col).getByte(recordIndex);
    }

    @Override
    public double getDouble(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return Double.NaN;
        }
        return colA(col).getDouble(recordIndex * 8);
    }

    @Override
    public float getFloat(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return Float.NaN;
        }
        return colA(col).getFloat(recordIndex * 4);
    }

    @Override
    public int getInt(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return Numbers.INT_NaN;
        }
        return colA(col).getInt(recordIndex * 4);
    }

    @Override
    public long getLong(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return Numbers.LONG_NaN;
        }
        return colA(col).getLong(recordIndex * 8);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(reader.getPartitionIndex(columnBase), recordIndex);
    }

    @Override
    public short getShort(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return 0;
        }
        return colA(col).getShort(recordIndex * 2);
    }

    @Override
    public char getChar(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return 0;
        }
        return colA(col).getChar(recordIndex * 2);
    }

    @Override
    public CharSequence getStr(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return null;
        }
        return colA(col).getStr(colB(col).getLong(recordIndex * 8));
    }

    @Override
    public void getLong256(int columnIndex, CharSink sink) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return;
        }
        colA(col).getLong256(recordIndex * Long256.BYTES, sink);
    }

    @Override
    public Long256 getLong256A(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return null;
        }
        return colA(col).getLong256A(recordIndex * Long256.BYTES);
    }

    @Override
    public Long256 getLong256B(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return null;
        }
        return colA(col).getLong256B(recordIndex * Long256.BYTES);
    }

    @Override
    public CharSequence getStrB(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return null;
        }
        return colA(col).getStr2(colB(col).getLong(recordIndex * 8));
    }

    @Override
    public int getStrLen(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return TableUtils.NULL_LEN;
        }
        return colA(col).getStrLen(colB(col).getLong(recordIndex * 8));
    }

    @Override
    public CharSequence getSym(int columnIndex) {
        final int col = deferenceColumn(columnIndex);
        final long recordIndex = getRecordIndex(col);
        if (recordIndex < 0) {
            return null;
        }
        return reader.getSymbolMapReader(col).valueOf(colA(col).getInt(recordIndex * 4));
    }

    public long getRecordIndex() {
        return recordIndex;
    }

    public void setRecordIndex(long recordIndex) {
        this.recordIndex = recordIndex;
    }

    public void incrementRecordIndex() {
        recordIndex++;
    }

    public void jumpTo(int partitionIndex, long recordIndex) {
        this.columnBase = reader.getColumnBase(partitionIndex);
        this.recordIndex = recordIndex;
    }

    public void of(TableReader reader) {
        this.reader = reader;
    }

    private ReadOnlyColumn colA(int col) {
        return reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, col));
    }

    private ReadOnlyColumn colB(int col) {
        return reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, col) + 1);
    }

    private int deferenceColumn(int columnIndex) {
        return columnIndexes.getQuick(columnIndex);
    }

    private long getRecordIndex(int col) {
        assert col > -1 && col < reader.getColumnCount() : "Column index out of bounds: " + col + " >= " + reader.getColumnCount();
        final long top = reader.getColumnTop(columnBase, col);
        return top > 0L ? recordIndex - top : recordIndex;
    }
}
