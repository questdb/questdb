/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.Record;

/**
 * Wraps over slave key symbols to master Record.
 * Why is it useful? LT and ASOF joins copy slave key columns to a map. When a symbol column
 * happened to be a key column, it is also converted to string. This is needed as 2 maps
 * have different symbol types, and we want to join to symbol string values. But if we read these columns
 * from the map then symbols are converted to strings, that's undesirable. So we wrap over slave key columns
 * to master record, so that we can read symbols from master record.
 */
public final class SymbolWrapOverJoinRecord extends OuterJoinRecord {
    private final ColumnFilter keyColumnsToMaster;
    private final int slaveValuesKeysSplit;

    public SymbolWrapOverJoinRecord(int masterSlaveSplit, Record nullRecord, int slaveValuesKeysSplit, ColumnFilter keyColumnsToMaster) {
        super(masterSlaveSplit, nullRecord);
        this.keyColumnsToMaster = keyColumnsToMaster;
        this.slaveValuesKeysSplit = slaveValuesKeysSplit;
    }

    @Override
    public int getInt(int col) {
        // symbols are internally represented as ints. so we need to wrap over ints too.
        if (col < split) {
            return master.getInt(col);
        }
        int slaveCol = col - split;
        if (shouldWrapOver(slaveCol)) {
            slaveCol -= slaveValuesKeysSplit;
            int masterCol = keyColumnsToMaster.getColumnIndexFactored(slaveCol);
            return master.getInt(masterCol);
        }
        return slave.getInt(slaveCol);
    }

    @Override
    public CharSequence getSym(int col) {
        if (col < split) {
            return master.getSym(col);
        }
        int slaveCol = col - split;
        if (shouldWrapOver(slaveCol)) {
            slaveCol -= slaveValuesKeysSplit;
            int masterCol = keyColumnsToMaster.getColumnIndexFactored(slaveCol);
            return master.getSym(masterCol);
        }
        return slave.getSym(slaveCol);
    }

    @Override
    public CharSequence getSymB(int col) {
        if (col < split) {
            return master.getSymB(col);
        }
        int slaveCol = col - split;
        if (shouldWrapOver(slaveCol)) {
            slaveCol -= slaveValuesKeysSplit;
            int masterCol = keyColumnsToMaster.getColumnIndexFactored(slaveCol);
            return master.getSymB(masterCol);
        }
        return slave.getSymB(slaveCol);
    }

    private boolean shouldWrapOver(int slaveCol) {
        return slaveCol >= slaveValuesKeysSplit // is key column
                && slave != nullRecord;
    }
}
