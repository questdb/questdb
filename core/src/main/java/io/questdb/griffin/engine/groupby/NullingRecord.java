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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

/**
 * Wraps a source Record and returns NULL values for "nulled" columns.
 * Used by GROUPING SETS to mask out columns that are not part of the
 * current grouping set, so the map key treats them as identical NULLs.
 */
public class NullingRecord implements Record {
    private Record base;
    // O(1) lookup: true at index i means column i is nulled.
    private boolean[] isColumnNulled;

    @Override
    public ArrayView getArray(int col, int columnType) {
        return isNulled(col) ? ArrayConstant.NULL : base.getArray(col, columnType);
    }

    @Override
    public BinarySequence getBin(int col) {
        return isNulled(col) ? null : base.getBin(col);
    }

    @Override
    public long getBinLen(int col) {
        return isNulled(col) ? -1 : base.getBinLen(col);
    }

    @Override
    public boolean getBool(int col) {
        return !isNulled(col) && base.getBool(col);
    }

    @Override
    public byte getByte(int col) {
        return isNulled(col) ? 0 : base.getByte(col);
    }

    @Override
    public char getChar(int col) {
        return isNulled(col) ? 0 : base.getChar(col);
    }

    @Override
    public long getDate(int col) {
        return isNulled(col) ? Numbers.LONG_NULL : base.getDate(col);
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        if (isNulled(col)) {
            sink.ofRawNull();
        } else {
            base.getDecimal128(col, sink);
        }
    }

    @Override
    public short getDecimal16(int col) {
        return isNulled(col) ? Decimals.DECIMAL16_NULL : base.getDecimal16(col);
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        if (isNulled(col)) {
            sink.ofRawNull();
        } else {
            base.getDecimal256(col, sink);
        }
    }

    @Override
    public int getDecimal32(int col) {
        return isNulled(col) ? Decimals.DECIMAL32_NULL : base.getDecimal32(col);
    }

    @Override
    public long getDecimal64(int col) {
        return isNulled(col) ? Decimals.DECIMAL64_NULL : base.getDecimal64(col);
    }

    @Override
    public byte getDecimal8(int col) {
        return isNulled(col) ? Decimals.DECIMAL8_NULL : base.getDecimal8(col);
    }

    @Override
    public double getDouble(int col) {
        return isNulled(col) ? Double.NaN : base.getDouble(col);
    }

    @Override
    public float getFloat(int col) {
        return isNulled(col) ? Float.NaN : base.getFloat(col);
    }

    @Override
    public byte getGeoByte(int col) {
        return isNulled(col) ? 0 : base.getGeoByte(col);
    }

    @Override
    public int getGeoInt(int col) {
        return isNulled(col) ? Numbers.INT_NULL : base.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        return isNulled(col) ? Numbers.LONG_NULL : base.getGeoLong(col);
    }

    @Override
    public short getGeoShort(int col) {
        return isNulled(col) ? 0 : base.getGeoShort(col);
    }

    @Override
    public int getIPv4(int col) {
        return isNulled(col) ? Numbers.IPv4_NULL : base.getIPv4(col);
    }

    @Override
    public int getInt(int col) {
        return isNulled(col) ? Numbers.INT_NULL : base.getInt(col);
    }

    @Override
    public Interval getInterval(int col) {
        return isNulled(col) ? Interval.NULL : base.getInterval(col);
    }

    @Override
    public long getLong(int col) {
        return isNulled(col) ? Numbers.LONG_NULL : base.getLong(col);
    }

    @Override
    public long getLong128Hi(int col) {
        return isNulled(col) ? Numbers.LONG_NULL : base.getLong128Hi(col);
    }

    @Override
    public long getLong128Lo(int col) {
        return isNulled(col) ? Numbers.LONG_NULL : base.getLong128Lo(col);
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        if (!isNulled(col)) {
            base.getLong256(col, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        return isNulled(col) ? Long256Impl.NULL_LONG256 : base.getLong256A(col);
    }

    @Override
    public Long256 getLong256B(int col) {
        return isNulled(col) ? Long256Impl.NULL_LONG256 : base.getLong256B(col);
    }

    @Override
    public long getRowId() {
        return base.getRowId();
    }

    @Override
    public short getShort(int col) {
        return isNulled(col) ? 0 : base.getShort(col);
    }

    @Override
    public CharSequence getStrA(int col) {
        return isNulled(col) ? null : base.getStrA(col);
    }

    @Override
    public CharSequence getStrB(int col) {
        return isNulled(col) ? null : base.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        return isNulled(col) ? -1 : base.getStrLen(col);
    }

    @Override
    public CharSequence getSymA(int col) {
        return isNulled(col) ? null : base.getSymA(col);
    }

    @Override
    public CharSequence getSymB(int col) {
        return isNulled(col) ? null : base.getSymB(col);
    }

    @Override
    public long getTimestamp(int col) {
        return isNulled(col) ? Numbers.LONG_NULL : base.getTimestamp(col);
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        return isNulled(col) ? null : base.getVarcharA(col);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        return isNulled(col) ? null : base.getVarcharB(col);
    }

    @Override
    public int getVarcharSize(int col) {
        return isNulled(col) ? -1 : base.getVarcharSize(col);
    }

    public void of(Record base, IntList nulledColumns) {
        this.base = base;
        if (nulledColumns == null || nulledColumns.size() == 0) {
            this.isColumnNulled = null;
        } else {
            int maxCol = 0;
            for (int i = 0, n = nulledColumns.size(); i < n; i++) {
                maxCol = Math.max(maxCol, nulledColumns.getQuick(i));
            }
            int requiredLength = maxCol + 1;
            if (this.isColumnNulled == null || this.isColumnNulled.length < requiredLength) {
                this.isColumnNulled = new boolean[requiredLength];
            } else {
                for (int i = 0, n = this.isColumnNulled.length; i < n; i++) {
                    this.isColumnNulled[i] = false;
                }
            }
            for (int i = 0, n = nulledColumns.size(); i < n; i++) {
                isColumnNulled[nulledColumns.getQuick(i)] = true;
            }
        }
    }

    private boolean isNulled(int col) {
        return isColumnNulled != null && col < isColumnNulled.length && isColumnNulled[col];
    }
}
