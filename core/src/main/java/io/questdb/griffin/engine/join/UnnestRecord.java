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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

/**
 * Composite record that combines base table columns (col &lt; split)
 * with unnested columns (col &gt;= split). The unnested region maps
 * each column index to a (source, sourceCol) pair via flat lookup
 * arrays. When WITH ORDINALITY is used, an {@link OrdinalityUnnestSource}
 * is included in the sources array and mapped like any other column.
 */
public class UnnestRecord implements Record {
    private final int[] colToSourceCol;
    private final int[] colToSourceIndex;
    private final ObjList<UnnestSource> sources;
    private final int split;
    private int arrayIndex;
    private Record baseRecord;

    public UnnestRecord(
            int split,
            ObjList<UnnestSource> sources
    ) {
        this.split = split;
        this.sources = sources;
        // Compute total unnest column count and build column mappings.
        int totalCols = 0;
        for (int i = 0, n = sources.size(); i < n; i++) {
            totalCols += sources.getQuick(i).getColumnCount();
        }
        this.colToSourceIndex = new int[totalCols];
        this.colToSourceCol = new int[totalCols];
        int idx = 0;
        for (int i = 0, n = sources.size(); i < n; i++) {
            int count = sources.getQuick(i).getColumnCount();
            for (int j = 0; j < count; j++) {
                colToSourceIndex[idx] = i;
                colToSourceCol[idx] = j;
                idx++;
            }
        }
    }

    @Override
    public ArrayView getArray(int col, int columnType) {
        if (col < split) {
            return baseRecord.getArray(col, columnType);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getArray(srcCol, arrayIndex, columnType);
    }

    // BINARY is not a supported unnest column type; col is always a base table column.
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
        if (col < split) {
            return baseRecord.getBool(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getBool(srcCol, arrayIndex);
    }

    // BYTE is never an unnest column type, so col is always a base table column.
    @Override
    public byte getByte(int col) {
        return baseRecord.getByte(col);
    }

    // CHAR is never an unnest column type, so col is always a base table column.
    @Override
    public char getChar(int col) {
        return baseRecord.getChar(col);
    }

    @Override
    public long getDate(int col) {
        if (col < split) {
            return baseRecord.getDate(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getDate(srcCol, arrayIndex);
    }

    // DECIMAL types are not supported unnest column types; col is always a base table column.
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
        if (col < split) {
            return baseRecord.getDouble(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getDouble(srcCol, arrayIndex);
    }

    // FLOAT is not a supported unnest column type; col is always a base table column.
    @Override
    public float getFloat(int col) {
        return baseRecord.getFloat(col);
    }

    // GEO types are not supported unnest column types; col is always a base table column.
    @Override
    public byte getGeoByte(int col) {
        return baseRecord.getGeoByte(col);
    }

    @Override
    public int getGeoInt(int col) {
        return baseRecord.getGeoInt(col);
    }

    @Override
    public long getGeoLong(int col) {
        return baseRecord.getGeoLong(col);
    }

    @Override
    public short getGeoShort(int col) {
        return baseRecord.getGeoShort(col);
    }

    // IPv4 is not a supported unnest column type; col is always a base table column.
    @Override
    public int getIPv4(int col) {
        return baseRecord.getIPv4(col);
    }

    @Override
    public int getInt(int col) {
        if (col < split) {
            return baseRecord.getInt(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getInt(srcCol, arrayIndex);
    }

    // INTERVAL is not a supported unnest column type; col is always a base table column.
    @Override
    public Interval getInterval(int col) {
        return baseRecord.getInterval(col);
    }

    @Override
    public long getLong(int col) {
        if (col < split) {
            return baseRecord.getLong(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getLong(srcCol, arrayIndex);
    }

    // LONG128, LONG256 are not supported unnest column types; col is always a base table column.
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
    public long getRowId() {
        return baseRecord.getRowId();
    }

    @Override
    public short getShort(int col) {
        if (col < split) {
            return baseRecord.getShort(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getShort(srcCol, arrayIndex);
    }

    @Override
    public CharSequence getStrA(int col) {
        if (col < split) {
            return baseRecord.getStrA(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getStrA(srcCol, arrayIndex);
    }

    @Override
    public CharSequence getStrB(int col) {
        if (col < split) {
            return baseRecord.getStrB(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getStrB(srcCol, arrayIndex);
    }

    @Override
    public int getStrLen(int col) {
        if (col < split) {
            return baseRecord.getStrLen(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getStrLen(srcCol, arrayIndex);
    }

    // SYMBOL is not a supported unnest column type; col is always a base table column.
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
        if (col < split) {
            return baseRecord.getTimestamp(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getTimestamp(srcCol, arrayIndex);
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        if (col < split) {
            return baseRecord.getVarcharA(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getVarcharA(srcCol, arrayIndex);
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        if (col < split) {
            return baseRecord.getVarcharB(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getVarcharB(srcCol, arrayIndex);
    }

    @Override
    public int getVarcharSize(int col) {
        if (col < split) {
            return baseRecord.getVarcharSize(col);
        }
        int unnestCol = col - split;
        int srcIdx = colToSourceIndex[unnestCol];
        int srcCol = colToSourceCol[unnestCol];
        return sources.getQuick(srcIdx).getVarcharSize(srcCol, arrayIndex);
    }

    int getSplit() {
        return split;
    }

    void of(Record baseRecord) {
        this.baseRecord = baseRecord;
    }

    void setArrayIndex(int arrayIndex) {
        this.arrayIndex = arrayIndex;
    }
}
