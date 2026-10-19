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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

final class WindowLightRecord implements Record {
    private final IntList sourceMap;
    private Record base;
    private Record narrow;
    private long rowIndex = -1;

    WindowLightRecord(IntList sourceMap) {
        this.sourceMap = sourceMap;
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getArray(encoded, columnType) : narrow.getArray(-encoded - 1, columnType);
    }

    @Override
    public int getArrayDimLen(int col, int columnType, int dim) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getArrayDimLen(encoded, columnType, dim) : narrow.getArrayDimLen(-encoded - 1, columnType, dim);
    }

    @Override
    public double getArrayDouble1d2d(int col, int columnType, int idx0, int idx1) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getArrayDouble1d2d(encoded, columnType, idx0, idx1) : narrow.getArrayDouble1d2d(-encoded - 1, columnType, idx0, idx1);
    }

    @Override
    public BinarySequence getBin(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getBin(encoded) : narrow.getBin(-encoded - 1);
    }

    @Override
    public long getBinLen(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getBinLen(encoded) : narrow.getBinLen(-encoded - 1);
    }

    @Override
    public boolean getBool(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getBool(encoded) : narrow.getBool(-encoded - 1);
    }

    @Override
    public byte getByte(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getByte(encoded) : narrow.getByte(-encoded - 1);
    }

    @Override
    public char getChar(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getChar(encoded) : narrow.getChar(-encoded - 1);
    }

    @Override
    public long getDate(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getDate(encoded) : narrow.getDate(-encoded - 1);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        int encoded = sourceMap.getQuick(col);
        if (encoded >= 0) {
            base.getDecimal128(encoded, sink);
        } else {
            narrow.getDecimal128(-encoded - 1, sink);
        }
    }

    @Override
    public short getDecimal16(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getDecimal16(encoded) : narrow.getDecimal16(-encoded - 1);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        int encoded = sourceMap.getQuick(col);
        if (encoded >= 0) {
            base.getDecimal256(encoded, sink);
        } else {
            narrow.getDecimal256(-encoded - 1, sink);
        }
    }

    @Override
    public int getDecimal32(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getDecimal32(encoded) : narrow.getDecimal32(-encoded - 1);
    }

    @Override
    public long getDecimal64(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getDecimal64(encoded) : narrow.getDecimal64(-encoded - 1);
    }

    @Override
    public byte getDecimal8(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getDecimal8(encoded) : narrow.getDecimal8(-encoded - 1);
    }

    @Override
    public double getDouble(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getDouble(encoded) : narrow.getDouble(-encoded - 1);
    }

    @Override
    public float getFloat(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getFloat(encoded) : narrow.getFloat(-encoded - 1);
    }

    @Override
    public byte getGeoByte(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getGeoByte(encoded) : narrow.getGeoByte(-encoded - 1);
    }

    @Override
    public int getGeoInt(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getGeoInt(encoded) : narrow.getGeoInt(-encoded - 1);
    }

    @Override
    public long getGeoLong(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getGeoLong(encoded) : narrow.getGeoLong(-encoded - 1);
    }

    @Override
    public short getGeoShort(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getGeoShort(encoded) : narrow.getGeoShort(-encoded - 1);
    }

    @Override
    public int getIPv4(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getIPv4(encoded) : narrow.getIPv4(-encoded - 1);
    }

    @Override
    public int getInt(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getInt(encoded) : narrow.getInt(-encoded - 1);
    }

    @Override
    public Interval getInterval(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getInterval(encoded) : narrow.getInterval(-encoded - 1);
    }

    @Override
    public long getLong(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getLong(encoded) : narrow.getLong(-encoded - 1);
    }

    @Override
    public long getLong128Hi(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getLong128Hi(encoded) : narrow.getLong128Hi(-encoded - 1);
    }

    @Override
    public long getLong128Lo(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getLong128Lo(encoded) : narrow.getLong128Lo(-encoded - 1);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        int encoded = sourceMap.getQuick(col);
        if (encoded >= 0) {
            base.getLong256(encoded, sink);
        } else {
            narrow.getLong256(-encoded - 1, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getLong256A(encoded) : narrow.getLong256A(-encoded - 1);
    }

    @Override
    public Long256 getLong256B(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getLong256B(encoded) : narrow.getLong256B(-encoded - 1);
    }

    @Override
    public long getRowId() {
        return rowIndex;
    }

    @Override
    public short getShort(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getShort(encoded) : narrow.getShort(-encoded - 1);
    }

    @Override
    public CharSequence getStrA(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getStrA(encoded) : narrow.getStrA(-encoded - 1);
    }

    @Override
    public CharSequence getStrB(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getStrB(encoded) : narrow.getStrB(-encoded - 1);
    }

    @Override
    public int getStrLen(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getStrLen(encoded) : narrow.getStrLen(-encoded - 1);
    }

    @Override
    public CharSequence getSymA(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getSymA(encoded) : narrow.getSymA(-encoded - 1);
    }

    @Override
    public CharSequence getSymB(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getSymB(encoded) : narrow.getSymB(-encoded - 1);
    }

    @Override
    public long getTimestamp(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getTimestamp(encoded) : narrow.getTimestamp(-encoded - 1);
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getVarcharA(encoded) : narrow.getVarcharA(-encoded - 1);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getVarcharB(encoded) : narrow.getVarcharB(-encoded - 1);
    }

    @Override
    public int getVarcharSize(int col) {
        int encoded = sourceMap.getQuick(col);
        return encoded >= 0 ? base.getVarcharSize(encoded) : narrow.getVarcharSize(-encoded - 1);
    }

    void of(Record base, Record narrow, long rowIndex) {
        this.base = base;
        this.narrow = narrow;
        this.rowIndex = rowIndex;
    }

    void setRowIndex(long rowIndex) {
        this.rowIndex = rowIndex;
    }
}
