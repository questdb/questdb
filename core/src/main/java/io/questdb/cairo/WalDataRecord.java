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

import static io.questdb.cairo.WalReader.getPrimaryColumnIndex;

public class WalDataRecord implements Record, Sinkable {
    private long recordIndex = 0;
    private WalReader reader;

    @Override
    public BinarySequence getBin(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getBin(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public long getBinLen(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getBinLen(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public boolean getBool(int col) {
        final long offset = recordIndex;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getBool(offset);
    }

    @Override
    public byte getByte(int col) {
        final long offset = recordIndex;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public double getDouble(int col) {
        final long offset = recordIndex * Double.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getDouble(offset);
    }

    @Override
    public float getFloat(int col) {
        final long offset = recordIndex * Float.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getFloat(offset);
    }

    @Override
    public int getInt(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getLong(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public short getShort(int col) {
        final long offset = recordIndex * Short.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public char getChar(int col) {
        final long offset = recordIndex * Character.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getChar(offset);
    }

    @Override
    public CharSequence getStr(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getStr(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public void getLong256(int col, CharSink sink) {
        final long offset = recordIndex * Long256.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        reader.getColumn(absoluteColumnIndex).getLong256(offset, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        final long offset = recordIndex * Long256.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong256A(offset);
    }

    @Override
    public Long256 getLong256B(int col) {
        final long offset = recordIndex * Long256.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong256B(offset);
    }

    @Override
    public CharSequence getStrB(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getStr2(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public int getStrLen(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getStrLen(reader.getColumn(absoluteColumnIndex + 1).getLong(offset));
    }

    @Override
    public CharSequence getSym(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getSymbolMapReader(col).valueOf(reader.getColumn(absoluteColumnIndex).getInt(offset));
    }

    @Override
    public CharSequence getSymB(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getSymbolMapReader(col).valueBOf(reader.getColumn(absoluteColumnIndex).getInt(offset));
    }

    @Override
    public byte getGeoByte(int col) {
        final long offset = recordIndex * Byte.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.BYTE_NULL : reader.getColumn(absoluteColumnIndex).getByte(offset);
    }

    @Override
    public short getGeoShort(int col) {
        final long offset = recordIndex * Short.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.SHORT_NULL : reader.getColumn(absoluteColumnIndex).getShort(offset);
    }

    @Override
    public int getGeoInt(int col) {
        final long offset = recordIndex * Integer.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.INT_NULL : reader.getColumn(absoluteColumnIndex).getInt(offset);
    }

    @Override
    public long getGeoLong(int col) {
        final long offset = recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return offset < 0 ? GeoHashes.NULL : reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public long getTimestamp(int col) {
        return col == reader.getTimestampIndex() ? getDesignatedTimestamp(col) : getLong(col);
    }

    private long getDesignatedTimestamp(int col) {
        final long offset = 2 * recordIndex * Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    // only for tests
    long getDesignatedTimestampRowId(int col) {
        final long offset = 2 * recordIndex * Long.BYTES + Long.BYTES;
        final int absoluteColumnIndex = getPrimaryColumnIndex(col);
        return reader.getColumn(absoluteColumnIndex).getLong(offset);
    }

    @Override
    public long getRowId() {
        return Rows.toRowID(0, recordIndex);
    }

    @Override
    public long getUpdateRowId() {
        throw new UnsupportedOperationException("UPDATE is not supported in WAL");
    }

    public long getRecordIndex() {
        return recordIndex;
    }

    public void incrementRecordIndex() {
        recordIndex++;
    }

    public void jumpTo(long recordIndex) {
        this.recordIndex = recordIndex;
    }

    public void of(WalReader reader) {
        this.reader = reader;
        jumpTo(-1);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("WalReaderRecord [recordIndex=").put(recordIndex).put(']');
    }
}
