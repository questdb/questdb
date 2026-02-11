/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

/**
 * Combined record that maps baseMetadata column indices to source records.
 * Used for markout GROUP BY queries where each row combines data from:
 * - Master record (from page frame)
 * - Sequence offset value (from long_sequence)
 * - Slave record (from ASOF JOIN)
 */
public class HorizonJoinRecord implements Record {
    public static final int SOURCE_MASTER = 0;
    public static final int SOURCE_SEQUENCE = 1;
    public static final int SOURCE_SLAVE = 2;

    private int[] columnIndices;
    private int[] columnSources;
    private long horizonTimestamp;
    private Record masterRecord;
    private long offsetValue;
    private Record slaveRecord;

    @Override
    public @Nullable ArrayView getArray(int col, int columnType) {
        Record src = getSourceRecord(col);
        return src != null ? src.getArray(columnIndices[col], columnType) : null;
    }

    @Override
    public @Nullable BinarySequence getBin(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getBin(columnIndices[col]) : null;
    }

    @Override
    public long getBinLen(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getBinLen(columnIndices[col]) : -1;
    }

    @Override
    public boolean getBool(int col) {
        Record src = getSourceRecord(col);
        return src != null && src.getBool(columnIndices[col]);
    }

    @Override
    public byte getByte(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getByte(columnIndices[col]) : 0;
    }

    @Override
    public char getChar(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getChar(columnIndices[col]) : 0;
    }

    @Override
    public long getDate(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getDate(columnIndices[col]) : Numbers.LONG_NULL;
    }

    @Override
    public void getDecimal128(int col, Decimal128 sink) {
        Record src = getSourceRecord(col);
        if (src != null) {
            src.getDecimal128(columnIndices[col], sink);
        }
    }

    @Override
    public short getDecimal16(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getDecimal16(columnIndices[col]) : 0;
    }

    @Override
    public void getDecimal256(int col, Decimal256 sink) {
        Record src = getSourceRecord(col);
        if (src != null) {
            src.getDecimal256(columnIndices[col], sink);
        }
    }

    @Override
    public int getDecimal32(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getDecimal32(columnIndices[col]) : Numbers.INT_NULL;
    }

    @Override
    public long getDecimal64(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getDecimal64(columnIndices[col]) : Numbers.LONG_NULL;
    }

    @Override
    public byte getDecimal8(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getDecimal8(columnIndices[col]) : 0;
    }

    @Override
    public double getDouble(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getDouble(columnIndices[col]) : Double.NaN;
    }

    @Override
    public float getFloat(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getFloat(columnIndices[col]) : Float.NaN;
    }

    @Override
    public byte getGeoByte(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getGeoByte(columnIndices[col]) : 0;
    }

    @Override
    public int getGeoInt(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getGeoInt(columnIndices[col]) : 0;
    }

    @Override
    public long getGeoLong(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getGeoLong(columnIndices[col]) : 0;
    }

    @Override
    public short getGeoShort(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getGeoShort(columnIndices[col]) : 0;
    }

    @Override
    public int getIPv4(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getIPv4(columnIndices[col]) : 0;
    }

    @Override
    public int getInt(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getInt(columnIndices[col]) : Numbers.INT_NULL;
    }

    @Override
    public @Nullable Interval getInterval(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getInterval(columnIndices[col]) : null;
    }

    @Override
    public long getLong(int col) {
        // Special handling for sequence offset - it's a direct value, not from a record
        if (columnSources[col] == SOURCE_SEQUENCE) {
            return offsetValue;
        }
        Record src = getSourceRecord(col);
        return src != null ? src.getLong(columnIndices[col]) : Numbers.LONG_NULL;
    }

    @Override
    public long getLong128Hi(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getLong128Hi(columnIndices[col]) : Numbers.LONG_NULL;
    }

    @Override
    public long getLong128Lo(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getLong128Lo(columnIndices[col]) : Numbers.LONG_NULL;
    }

    @Override
    public void getLong256(int col, CharSink<?> sink) {
        Record src = getSourceRecord(col);
        if (src != null) {
            src.getLong256(columnIndices[col], sink);
        }
    }

    @Override
    public @Nullable Long256 getLong256A(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getLong256A(columnIndices[col]) : null;
    }

    @Override
    public @Nullable Long256 getLong256B(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getLong256B(columnIndices[col]) : null;
    }

    @Override
    public @Nullable Record getRecord(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getRecord(columnIndices[col]) : null;
    }

    @Override
    public long getRowId() {
        return masterRecord != null ? masterRecord.getRowId() : Numbers.LONG_NULL;
    }

    @Override
    public short getShort(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getShort(columnIndices[col]) : 0;
    }

    @Override
    public @Nullable CharSequence getStrA(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getStrA(columnIndices[col]) : null;
    }

    @Override
    public @Nullable CharSequence getStrB(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getStrB(columnIndices[col]) : null;
    }

    @Override
    public int getStrLen(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getStrLen(columnIndices[col]) : -1;
    }

    @Override
    public @Nullable CharSequence getSymA(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getSymA(columnIndices[col]) : null;
    }

    @Override
    public @Nullable CharSequence getSymB(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getSymB(columnIndices[col]) : null;
    }

    @Override
    public long getTimestamp(int col) {
        // Special handling for horizon timestamp - it's stored directly, not from a record
        if (columnSources[col] == SOURCE_SEQUENCE) {
            // columnIndices[col] == 1 for timestamp column (0 for offset)
            return horizonTimestamp;
        }
        Record src = getSourceRecord(col);
        return src != null ? src.getTimestamp(columnIndices[col]) : Numbers.LONG_NULL;
    }

    @Override
    public long getUpdateRowId() {
        return masterRecord != null ? masterRecord.getUpdateRowId() : Numbers.LONG_NULL;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getVarcharA(columnIndices[col]) : null;
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getVarcharB(columnIndices[col]) : null;
    }

    @Override
    public int getVarcharSize(int col) {
        Record src = getSourceRecord(col);
        return src != null ? src.getVarcharSize(columnIndices[col]) : -1;
    }

    /**
     * Initializes the column mappings. Called once per reduce() call.
     */
    public void init(int[] columnSources, int[] columnIndices) {
        this.columnSources = columnSources;
        this.columnIndices = columnIndices;
    }

    /**
     * Sets the source records for this row.
     *
     * @param masterRecord     the master record from the page frame
     * @param offsetValue      the offset value (horizon pseudo-table)
     * @param horizonTimestamp the computed horizon timestamp (master timestamp + offset; horizon pseudo-table)
     * @param slaveRecord      the matched slave record from ASOF JOIN (may be null)
     */
    public void of(Record masterRecord, long offsetValue, long horizonTimestamp, Record slaveRecord) {
        this.masterRecord = masterRecord;
        this.offsetValue = offsetValue;
        this.horizonTimestamp = horizonTimestamp;
        this.slaveRecord = slaveRecord;
    }

    private Record getSourceRecord(int col) {
        return switch (columnSources[col]) {
            case SOURCE_MASTER -> masterRecord;
            case SOURCE_SEQUENCE -> null; // Sequence values are handled specially in getLong()
            case SOURCE_SLAVE -> slaveRecord;
            default -> null;
        };
    }
}
