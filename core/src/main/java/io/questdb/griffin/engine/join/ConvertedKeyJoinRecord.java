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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

/**
 * Record of a full-fat ASOF or LT join whose map stores a right-side key column in a type other
 * than the column's own type.
 * <p>
 * The code generator picks a key type for every key pair, the map key stores the pair in that type,
 * and the join exposes each right-side key column from the map key. When the pair compares different
 * types, the right-side key copier converts the value for the comparison:
 * <ul>
 *     <li>it writes a STRING column compared with a VARCHAR column as VARCHAR;</li>
 *     <li>it writes a TIMESTAMP column compared with a TIMESTAMP_NS column in nanoseconds;</li>
 *     <li>it writes a SYMBOL column compared with the same column of the same table as its symbol
 *     key, while {@link SymbolWrapOverJoinRecord} reads a SYMBOL key column as the symbol string.</li>
 * </ul>
 * The join metadata keeps the column's own type, so this record reads such a column in the stored
 * type and returns it in the column's own type. It reads every other column as
 * {@link SymbolWrapOverJoinRecord} does, and the join creates it only when the map needs it.
 */
final class ConvertedKeyJoinRecord extends SymbolWrapOverJoinRecord {
    static final byte NO_CONVERSION = 0;
    static final byte STRING_AS_VARCHAR = 1;
    static final byte SYMBOL_AS_KEY = 2;
    static final byte TIMESTAMP_AS_OTHER_UNIT = 3;
    private final byte[] conversions;
    // index of the first exposed key column in this record
    private final int keySplit;
    private final int[] storedTypes;
    private final ObjList<TimestampDriver> timestampDrivers;
    private final ObjList<StringSink> utf16SinksA;
    private final ObjList<StringSink> utf16SinksB;

    ConvertedKeyJoinRecord(
            int masterSlaveSplit,
            Record nullRecord,
            int slaveValuesKeysSplit,
            ColumnFilter keyColumnsToMaster,
            byte[] conversions,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes slaveColumnTypes
    ) {
        super(masterSlaveSplit, nullRecord, slaveValuesKeysSplit, keyColumnsToMaster);
        this.conversions = conversions;
        this.keySplit = masterSlaveSplit + slaveValuesKeysSplit;
        final int keyCount = conversions.length;
        this.storedTypes = new int[keyCount];
        this.timestampDrivers = new ObjList<>(keyCount);
        this.utf16SinksA = new ObjList<>(keyCount);
        this.utf16SinksB = new ObjList<>(keyCount);
        for (int k = 0; k < keyCount; k++) {
            storedTypes[k] = mapKeyTypes.getColumnType(k);
            final boolean isStringAsVarchar = conversions[k] == STRING_AS_VARCHAR;
            utf16SinksA.add(isStringAsVarchar ? new StringSink() : null);
            utf16SinksB.add(isStringAsVarchar ? new StringSink() : null);
            timestampDrivers.add(
                    conversions[k] == TIMESTAMP_AS_OTHER_UNIT
                            ? ColumnType.getTimestampDriver(slaveColumnTypes.getColumnType(slaveValuesKeysSplit + k))
                            : null
            );
        }
    }

    @Override
    public long getLong(int col) {
        final int key = col - keySplit;
        if (key >= 0 && conversions[key] == TIMESTAMP_AS_OTHER_UNIT && slave != nullRecord) {
            return timestampDrivers.getQuick(key).from(slave.getLong(col - split), storedTypes[key]);
        }
        return super.getLong(col);
    }

    @Override
    public CharSequence getStrA(int col) {
        final int key = col - keySplit;
        if (key >= 0 && conversions[key] == STRING_AS_VARCHAR && slave != nullRecord) {
            return toUtf16(slave.getVarcharA(col - split), utf16SinksA.getQuick(key));
        }
        return super.getStrA(col);
    }

    @Override
    public CharSequence getStrB(int col) {
        final int key = col - keySplit;
        if (key >= 0 && conversions[key] == STRING_AS_VARCHAR && slave != nullRecord) {
            return toUtf16(slave.getVarcharB(col - split), utf16SinksB.getQuick(key));
        }
        return super.getStrB(col);
    }

    @Override
    public int getStrLen(int col) {
        final int key = col - keySplit;
        if (key >= 0 && conversions[key] == STRING_AS_VARCHAR && slave != nullRecord) {
            return TableUtils.lengthOf(toUtf16(slave.getVarcharA(col - split), utf16SinksA.getQuick(key)));
        }
        return super.getStrLen(col);
    }

    @Override
    public CharSequence getSymA(int col) {
        final int key = col - keySplit;
        if (key >= 0 && conversions[key] == SYMBOL_AS_KEY) {
            // the map record resolves the symbol key through the right-side symbol table
            return slave.getSymA(col - split);
        }
        return super.getSymA(col);
    }

    @Override
    public CharSequence getSymB(int col) {
        final int key = col - keySplit;
        if (key >= 0 && conversions[key] == SYMBOL_AS_KEY) {
            return slave.getSymB(col - split);
        }
        return super.getSymB(col);
    }

    @Override
    public long getTimestamp(int col) {
        final int key = col - keySplit;
        if (key >= 0 && conversions[key] == TIMESTAMP_AS_OTHER_UNIT && slave != nullRecord) {
            return timestampDrivers.getQuick(key).from(slave.getTimestamp(col - split), storedTypes[key]);
        }
        return super.getTimestamp(col);
    }

    private static CharSequence toUtf16(Utf8Sequence utf8, StringSink sink) {
        // the map encodes a STRING value as UTF-8, so the bytes always decode
        return utf8 != null ? Utf8s.utf8ToUtf16OrView(utf8, sink) : null;
    }

    static byte conversionOf(int columnType, int storedType) {
        if (columnType == storedType) {
            // a self-join on the same SYMBOL column is the only pair that keys the map on the symbol key
            return columnType == ColumnType.SYMBOL ? SYMBOL_AS_KEY : NO_CONVERSION;
        }
        if (columnType == ColumnType.STRING && storedType == ColumnType.VARCHAR) {
            return STRING_AS_VARCHAR;
        }
        if (ColumnType.isTimestamp(columnType) && ColumnType.isTimestamp(storedType)) {
            return TIMESTAMP_AS_OTHER_UNIT;
        }
        // SymbolWrapOverJoinRecord reads a SYMBOL key column stored as STRING
        assert columnType == ColumnType.SYMBOL && storedType == ColumnType.STRING
                : "unexpected join key conversion [columnType=" + ColumnType.nameOf(columnType)
                + ", storedType=" + ColumnType.nameOf(storedType) + ']';
        return NO_CONVERSION;
    }
}
