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
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;

public class UnnestRecord implements Record {
    private final int[] colToSourceCol;
    private final int[] colToSourceIndex;
    private final boolean hasOrdinality;
    private final UnnestSource[] sources;
    private final int split;
    private final int unnestColumnCount;
    private int arrayIndex;
    private Record baseRecord;

    public UnnestRecord(
            int split,
            UnnestSource[] sources,
            boolean hasOrdinality
    ) {
        this.split = split;
        this.sources = sources;
        this.hasOrdinality = hasOrdinality;
        // Compute total unnest column count and build column mappings.
        int totalCols = 0;
        for (int i = 0; i < sources.length; i++) {
            totalCols += sources[i].getColumnCount();
        }
        this.unnestColumnCount = totalCols;
        this.colToSourceIndex = new int[totalCols];
        this.colToSourceCol = new int[totalCols];
        int idx = 0;
        for (int i = 0; i < sources.length; i++) {
            int count = sources[i].getColumnCount();
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
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return null;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getArray(
                    srcCol, arrayIndex, columnType
            );
        }
        return null;
    }

    @Override
    public BinarySequence getBin(int col) {
        if (col < split) {
            return baseRecord.getBin(col);
        }
        return null;
    }

    @Override
    public long getBinLen(int col) {
        if (col < split) {
            return baseRecord.getBinLen(col);
        }
        return -1;
    }

    @Override
    public boolean getBool(int col) {
        if (col < split) {
            return baseRecord.getBool(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return false;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getBool(srcCol, arrayIndex);
        }
        return false;
    }

    @Override
    public byte getByte(int col) {
        if (col < split) {
            return baseRecord.getByte(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return 0;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getByte(srcCol, arrayIndex);
        }
        return 0;
    }

    @Override
    public char getChar(int col) {
        if (col < split) {
            return baseRecord.getChar(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return 0;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getChar(srcCol, arrayIndex);
        }
        return 0;
    }

    @Override
    public long getDate(int col) {
        if (col < split) {
            return baseRecord.getDate(col);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        if (col < split) {
            baseRecord.getDecimal128(col, sink);
        }
    }

    @Override
    public short getDecimal16(int col) {
        if (col < split) {
            return baseRecord.getDecimal16(col);
        }
        return 0;
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        if (col < split) {
            baseRecord.getDecimal256(col, sink);
        }
    }

    @Override
    public int getDecimal32(int col) {
        if (col < split) {
            return baseRecord.getDecimal32(col);
        }
        return Numbers.INT_NULL;
    }

    @Override
    public long getDecimal64(int col) {
        if (col < split) {
            return baseRecord.getDecimal64(col);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public byte getDecimal8(int col) {
        if (col < split) {
            return baseRecord.getDecimal8(col);
        }
        return 0;
    }

    @Override
    public double getDouble(int col) {
        if (col < split) {
            return baseRecord.getDouble(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return (double) (arrayIndex + 1);
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getDouble(srcCol, arrayIndex);
        }
        return Double.NaN;
    }

    @Override
    public float getFloat(int col) {
        if (col < split) {
            return baseRecord.getFloat(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return Float.NaN;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getFloat(srcCol, arrayIndex);
        }
        return Float.NaN;
    }

    @Override
    public byte getGeoByte(int col) {
        if (col < split) {
            return baseRecord.getGeoByte(col);
        }
        return 0;
    }

    @Override
    public int getGeoInt(int col) {
        if (col < split) {
            return baseRecord.getGeoInt(col);
        }
        return 0;
    }

    @Override
    public long getGeoLong(int col) {
        if (col < split) {
            return baseRecord.getGeoLong(col);
        }
        return 0;
    }

    @Override
    public short getGeoShort(int col) {
        if (col < split) {
            return baseRecord.getGeoShort(col);
        }
        return 0;
    }

    @Override
    public int getIPv4(int col) {
        if (col < split) {
            return baseRecord.getIPv4(col);
        }
        return Numbers.IPv4_NULL;
    }

    @Override
    public int getInt(int col) {
        if (col < split) {
            return baseRecord.getInt(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return Numbers.INT_NULL;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getInt(srcCol, arrayIndex);
        }
        return Numbers.INT_NULL;
    }

    @Override
    public Interval getInterval(int col) {
        if (col < split) {
            return baseRecord.getInterval(col);
        }
        return null;
    }

    @Override
    public long getLong(int col) {
        if (col < split) {
            return baseRecord.getLong(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return arrayIndex + 1; // 1-based ordinality
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getLong(srcCol, arrayIndex);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public long getLong128Hi(int col) {
        if (col < split) {
            return baseRecord.getLong128Hi(col);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public long getLong128Lo(int col) {
        if (col < split) {
            return baseRecord.getLong128Lo(col);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        if (col < split) {
            baseRecord.getLong256(col, sink);
        }
    }

    @Override
    public Long256 getLong256A(int col) {
        if (col < split) {
            return baseRecord.getLong256A(col);
        }
        return Long256Impl.NULL_LONG256;
    }

    @Override
    public Long256 getLong256B(int col) {
        if (col < split) {
            return baseRecord.getLong256B(col);
        }
        return Long256Impl.NULL_LONG256;
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
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return 0;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getShort(srcCol, arrayIndex);
        }
        return 0;
    }

    @Override
    public CharSequence getStrA(int col) {
        if (col < split) {
            return baseRecord.getStrA(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return null;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getStrA(srcCol, arrayIndex);
        }
        return null;
    }

    @Override
    public CharSequence getStrB(int col) {
        if (col < split) {
            return baseRecord.getStrB(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return null;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getStrB(srcCol, arrayIndex);
        }
        return null;
    }

    @Override
    public int getStrLen(int col) {
        if (col < split) {
            return baseRecord.getStrLen(col);
        }
        int unnestCol = col - split;
        if (hasOrdinality && unnestCol == unnestColumnCount) {
            return -1;
        }
        if (unnestCol < unnestColumnCount) {
            int srcIdx = colToSourceIndex[unnestCol];
            int srcCol = colToSourceCol[unnestCol];
            return sources[srcIdx].getStrLen(srcCol, arrayIndex);
        }
        return -1;
    }

    @Override
    public CharSequence getSymA(int col) {
        if (col < split) {
            return baseRecord.getSymA(col);
        }
        return null;
    }

    @Override
    public CharSequence getSymB(int col) {
        if (col < split) {
            return baseRecord.getSymB(col);
        }
        return null;
    }

    @Override
    public long getTimestamp(int col) {
        if (col < split) {
            return baseRecord.getTimestamp(col);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public Utf8Sequence getVarcharA(int col) {
        if (col < split) {
            return baseRecord.getVarcharA(col);
        }
        return null;
    }

    @Override
    public Utf8Sequence getVarcharB(int col) {
        if (col < split) {
            return baseRecord.getVarcharB(col);
        }
        return null;
    }

    @Override
    public int getVarcharSize(int col) {
        if (col < split) {
            return baseRecord.getVarcharSize(col);
        }
        return -1;
    }

    void of(Record baseRecord) {
        this.baseRecord = baseRecord;
    }

    void setArrayIndex(int arrayIndex) {
        this.arrayIndex = arrayIndex;
    }
}
