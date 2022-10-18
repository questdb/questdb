/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.Rows;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

public class TableReaderRecord implements Record, Sinkable {

    private int columnBase;
    private long recordIndex = 0;
    private TableReader reader;

    @Override
    public BinarySequence getBin(int col) {
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getBin(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public long getBinLen(int col) {
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getBinLen(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public boolean getBool(int col) {
        final long offset = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getBool(offset);
    }

    @Override
    public byte getByte(int col) {
        final long offset = getAdjustedRecordIndex(col);
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public double getDouble(int col) {
        final long offset = getAdjustedRecordIndex(col) * Double.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getDouble(offset);
    }

    @Override
    public float getFloat(int col) {
        final long offset = getAdjustedRecordIndex(col) * Float.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getFloat(offset);
    }

    @Override
    public int getInt(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getLong(int col) {
        final long offset = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(reader.getPartitionIndex(columnBase), recordIndex);
    }

    @Override
    public long getUpdateRowId() {
        return getRowId();
    }

    @Override
    public short getShort(int col) {
        final long offset = getAdjustedRecordIndex(col) * Short.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public char getChar(int col) {
        final long offset = getAdjustedRecordIndex(col) * Character.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getChar(offset);
    }

    @Override
    public CharSequence getStr(int col) {
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                recordIndex,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getColumn(absoluteColumnIndex).getStr(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        reader.getColumn(absoluteColumnIndex).getLong256(offset, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long offset = getAdjustedRecordIndex(col) * Long256.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(offset, index);
        return reader.getColumn(absoluteColumnIndex).getLong256B(offset);
    }

    @Override
    public CharSequence getStrB(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(recordIndex, index);
        return reader.getColumn(absoluteColumnIndex).getStr2(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public int getStrLen(int col) {
        final int index = TableReader.getPrimaryColumnIndex(columnBase, col);
        final long recordIndex = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(recordIndex, index);
        return reader.getColumn(absoluteColumnIndex).getStrLen(
                reader.getColumn(absoluteColumnIndex + 1).getLong(recordIndex)
        );
    }

    @Override
    public CharSequence getSym(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getSymbolMapReader(col).valueOf(reader.getColumn(absoluteColumnIndex).getInt(offset));
    }

    @Override
    public CharSequence getSymB(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = ifOffsetNegThen0ElseValue(
                offset,
                TableReader.getPrimaryColumnIndex(columnBase, col)
        );
        return reader.getSymbolMapReader(col).valueBOf(reader.getColumn(absoluteColumnIndex).getInt(offset));
    }

    @Override
    public byte getGeoByte(int col) {
        final long offset = getAdjustedRecordIndex(col) * Byte.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.BYTE_NULL : reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public short getGeoShort(int col) {
        final long offset = getAdjustedRecordIndex(col) * Short.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.SHORT_NULL : reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public int getGeoInt(int col) {
        final long offset = getAdjustedRecordIndex(col) * Integer.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.INT_NULL : reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getGeoLong(int col) {
        final long offset = getAdjustedRecordIndex(col) * Long.BYTES;
        final int absoluteColumnIndex = TableReader.getPrimaryColumnIndex(columnBase, col);
        return offset < 0 ? GeoHashes.NULL : reader.getColumn(absoluteColumnIndex).getLong(offset);
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

    public static int ifOffsetNegThen0ElseValue(long offset, int value) {
        return offset < 0 ? 0 : value;
    }

    private long getAdjustedRecordIndex(int col) {
        assert col > -1 && col < reader.getColumnCount() : "Column index out of bounds: " + col + " >= " + reader.getColumnCount();
        return recordIndex - reader.getColumnTop(columnBase, col);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("TableReaderRecord [columnBase=").put(columnBase).put(", recordIndex=").put(recordIndex).put(']');
    }
}
