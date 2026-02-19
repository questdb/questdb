/*******************************************************************************
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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectLongList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf16Sink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

class MaterializedRecord implements Record {
    private Record baseRecord;
    private DirectLongList[] buffers;
    private int[] colToBufferIndex;
    private int[] colTypes;
    private int ordinal;

    @Override
    public ArrayView getArray(int col, int columnType) {
        return baseRecord.getArray(col, columnType);
    }

    @Override
    public BinarySequence getBin(int col) {
        return baseRecord.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        return baseRecord.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return buffers[idx].get(ordinal) != 0L;
        }
        return baseRecord.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (byte) buffers[idx].get(ordinal);
        }
        return baseRecord.getByte(col);
    }

    @Override
    public char getChar(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (char) buffers[idx].get(ordinal);
        }
        return baseRecord.getChar(col);
    }

    @Override
    public long getDate(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return buffers[idx].get(ordinal);
        }
        return baseRecord.getDate(col);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        baseRecord.getDecimal128(col, sink);
    }

    @Override
    public short getDecimal16(int col) {
        return baseRecord.getDecimal16(col);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        baseRecord.getDecimal256(col, sink);
    }

    @Override
    public int getDecimal32(int col) {
        return baseRecord.getDecimal32(col);
    }

    @Override
    public long getDecimal64(int col) {
        return baseRecord.getDecimal64(col);
    }

    @Override
    public byte getDecimal8(int col) {
        return baseRecord.getDecimal8(col);
    }

    @Override
    public double getDouble(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return Double.longBitsToDouble(buffers[idx].get(ordinal));
        }
        return baseRecord.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return Float.intBitsToFloat((int) buffers[idx].get(ordinal));
        }
        return baseRecord.getFloat(col);
    }

    @Override
    public byte getGeoByte(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (byte) buffers[idx].get(ordinal);
        }
        return baseRecord.getGeoByte(col);
    }

    @Override
    public int getGeoInt(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (int) buffers[idx].get(ordinal);
        }
        return baseRecord.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return buffers[idx].get(ordinal);
        }
        return baseRecord.getGeoLong(col);
    }

    @Override
    public short getGeoShort(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (short) buffers[idx].get(ordinal);
        }
        return baseRecord.getGeoShort(col);
    }

    @Override
    public int getIPv4(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (int) buffers[idx].get(ordinal);
        }
        return baseRecord.getIPv4(col);
    }

    @Override
    public int getInt(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (int) buffers[idx].get(ordinal);
        }
        return baseRecord.getInt(col);
    }

    @Override
    public Interval getInterval(int col) {
        return baseRecord.getInterval(col);
    }

    @Override
    public long getLong(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return buffers[idx].get(ordinal);
        }
        return baseRecord.getLong(col);
    }

    @Override
    public long getLong128Hi(int col) {
        return baseRecord.getLong128Hi(col);
    }

    @Override
    public long getLong128Lo(int col) {
        return baseRecord.getLong128Lo(col);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        baseRecord.getLong256(col, sink);
    }

    @Override
    public Long256 getLong256A(int col) {
        return baseRecord.getLong256A(col);
    }

    @Override
    public Long256 getLong256B(int col) {
        return baseRecord.getLong256B(col);
    }

    @Override
    public long getLongIPv4(int col) {
        return baseRecord.getLongIPv4(col);
    }

    @Override
    public Record getRecord(int col) {
        return baseRecord.getRecord(col);
    }

    @Override
    public long getRowId() {
        return baseRecord.getRowId();
    }

    @Override
    public short getShort(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return (short) buffers[idx].get(ordinal);
        }
        return baseRecord.getShort(col);
    }

    @Override
    @Nullable
    public CharSequence getStrA(int col) {
        return baseRecord.getStrA(col);
    }

    @Override
    public CharSequence getStrB(int col) {
        return baseRecord.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        return baseRecord.getStrLen(col);
    }

    @Override
    public CharSequence getSymA(int col) {
        return baseRecord.getSymA(col);
    }

    @Override
    public CharSequence getSymB(int col) {
        return baseRecord.getSymB(col);
    }

    @Override
    public long getTimestamp(int col) {
        int idx = colToBufferIndex[col];
        if (idx >= 0) {
            return buffers[idx].get(ordinal);
        }
        return baseRecord.getTimestamp(col);
    }

    @Override
    public long getUpdateRowId() {
        return baseRecord.getUpdateRowId();
    }

    @Override
    public void getVarchar(int col, Utf16Sink utf16Sink) {
        baseRecord.getVarchar(col, utf16Sink);
    }

    @Override
    @Nullable
    public Utf8Sequence getVarcharA(int col) {
        return baseRecord.getVarcharA(col);
    }

    @Override
    @Nullable
    public Utf8Sequence getVarcharB(int col) {
        return baseRecord.getVarcharB(col);
    }

    @Override
    public int getVarcharSize(int col) {
        return baseRecord.getVarcharSize(col);
    }

    Record getBaseRecord() {
        return baseRecord;
    }

    void of(Record baseRecord, int[] colToBufferIndex, int[] colTypes, DirectLongList[] buffers) {
        this.baseRecord = baseRecord;
        this.colToBufferIndex = colToBufferIndex;
        this.colTypes = colTypes;
        this.buffers = buffers;
    }

    void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    static long extractValue(Record record, int colIndex, int colType) {
        return switch (ColumnType.tagOf(colType)) {
            case ColumnType.BOOLEAN -> record.getBool(colIndex) ? 1L : 0L;
            case ColumnType.BYTE -> record.getByte(colIndex);
            case ColumnType.SHORT -> record.getShort(colIndex);
            case ColumnType.CHAR -> record.getChar(colIndex);
            case ColumnType.INT -> record.getInt(colIndex);
            case ColumnType.IPv4 -> record.getIPv4(colIndex);
            case ColumnType.FLOAT -> Float.floatToRawIntBits(record.getFloat(colIndex));
            case ColumnType.LONG, ColumnType.TIMESTAMP, ColumnType.DATE -> record.getLong(colIndex);
            case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(colIndex));
            case ColumnType.GEOBYTE -> record.getGeoByte(colIndex);
            case ColumnType.GEOSHORT -> record.getGeoShort(colIndex);
            case ColumnType.GEOINT -> record.getGeoInt(colIndex);
            case ColumnType.GEOLONG -> record.getGeoLong(colIndex);
            default -> throw new UnsupportedOperationException("unsupported column type for materialization: " + ColumnType.nameOf(colType));
        };
    }
}
