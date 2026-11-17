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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Transient;

/**
 * Wraps over slave key symbols to master Record.
 * Why is it useful? LT and ASOF joins copy slave key columns to a map. When a symbol column
 * happened to be a key column, it is also converted to string. This is needed as 2 maps
 * have different symbol types, and we want to join to symbol string values. But if we read these columns
 * from the map then symbols are converted to strings, that's undesirable. So we wrap over slave key columns
 * to master record, so that we can read symbols from master record.
 */
public class SymbolWrapOverJoinRecord extends OuterJoinRecord {
    private final ColumnFilter keyColumnsToMaster;
    private final int slaveValuesKeysSplit;

    public SymbolWrapOverJoinRecord(int masterSlaveSplit, Record nullRecord, int slaveValuesKeysSplit, ColumnFilter keyColumnsToMaster) {
        super(masterSlaveSplit, nullRecord);
        this.keyColumnsToMaster = keyColumnsToMaster;
        this.slaveValuesKeysSplit = slaveValuesKeysSplit;
    }

    /**
     * Creates the record of a full-fat ASOF or LT join. The join map stores each right-side key
     * column in the key type of its key pair, which is not the column's own type when the pair
     * compares different types. For such a map this returns a {@link ConvertedKeyJoinRecord},
     * which reads the stored value back as the column's own type.
     *
     * @param mapKeyTypes      key types of the join map, the exposed key columns first
     * @param slaveColumnTypes right-side column types as the join exposes them: the map value
     *                         columns, then one column per exposed key column
     */
    public static SymbolWrapOverJoinRecord newInstance(
            int masterSlaveSplit,
            Record nullRecord,
            int slaveValuesKeysSplit,
            ColumnFilter keyColumnsToMaster,
            @Transient ColumnTypes mapKeyTypes,
            @Transient ColumnTypes slaveColumnTypes
    ) {
        final int keyCount = slaveColumnTypes.getColumnCount() - slaveValuesKeysSplit;
        byte[] conversions = null;
        for (int k = 0; k < keyCount; k++) {
            final byte conversion = ConvertedKeyJoinRecord.conversionOf(
                    slaveColumnTypes.getColumnType(slaveValuesKeysSplit + k),
                    mapKeyTypes.getColumnType(k)
            );
            if (conversion != ConvertedKeyJoinRecord.NO_CONVERSION) {
                if (conversions == null) {
                    conversions = new byte[keyCount];
                }
                conversions[k] = conversion;
            }
        }
        if (conversions == null) {
            return new SymbolWrapOverJoinRecord(masterSlaveSplit, nullRecord, slaveValuesKeysSplit, keyColumnsToMaster);
        }
        return new ConvertedKeyJoinRecord(
                masterSlaveSplit,
                nullRecord,
                slaveValuesKeysSplit,
                keyColumnsToMaster,
                conversions,
                mapKeyTypes,
                slaveColumnTypes
        );
    }

    @Override
    public int getInt(int col) {
        if (col < split) {
            // it's an int from master record
            // no need to do anything special
            return master.getInt(col);
        }
        // ok, it's an int from slave record
        int slaveCol = col - split;
        if (shouldWrapOver(slaveCol)) {
            // we need to wrap over to master record
            // because it could be a symbol column
            slaveCol -= slaveValuesKeysSplit;
            int masterCol = keyColumnsToMaster.getColumnIndexFactored(slaveCol);
            return master.getInt(masterCol);
        }
        return slave.getInt(slaveCol);
    }

    @Override
    public CharSequence getSymA(int col) {
        if (col < split) {
            return master.getSymA(col);
        }
        int slaveCol = col - split;
        if (isSlaveKeyColumn(slaveCol)) {
            // key symbols are converted to strings before inserting into map.
            // so we can read them as strings directly from the map.
            return slave.getStrA(slaveCol);
        }
        return slave.getSymA(slaveCol);
    }

    @Override
    public CharSequence getSymB(int col) {
        if (col < split) {
            return master.getSymB(col);
        }
        int slaveCol = col - split;
        if (isSlaveKeyColumn(slaveCol)) {
            return slave.getStrB(slaveCol);
        } else {
            return slave.getSymB(slaveCol);
        }
    }

    private boolean isSlaveKeyColumn(int col) {
        return col >= slaveValuesKeysSplit;
    }

    private boolean shouldWrapOver(int slaveCol) {
        return slaveCol >= slaveValuesKeysSplit // is key column
                && slave != nullRecord;
    }
}
