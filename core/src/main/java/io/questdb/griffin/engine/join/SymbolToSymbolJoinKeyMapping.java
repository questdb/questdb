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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.std.IntIntHashMap;

public final class SymbolToSymbolJoinKeyMapping implements SymbolJoinKeyMapping, SymbolShortCircuit {
    private final CairoConfiguration config;
    private final IntIntHashMap masterKeyToSlaveKey = new IntIntHashMap(16, 0.4);
    private final int masterSymbolIndex;
    private final int slaveSymbolIndex;
    private int maxCacheSize = 0;
    private StaticSymbolTable slaveSymbolTable;

    public SymbolToSymbolJoinKeyMapping(CairoConfiguration config, int masterSymbolIndex, int slaveSymbolIndex) {
        this.config = config;
        this.masterSymbolIndex = masterSymbolIndex;
        this.slaveSymbolIndex = slaveSymbolIndex;
    }

    @Override
    public int getSlaveKey(Record masterRecord) {
        assert slaveSymbolTable != null : "slaveSymbolTable must be set before calling getSlaveKey";

        int masterKey = masterRecord.getInt(masterSymbolIndex);
        int slaveKey = masterKeyToSlaveKey.get(masterKey);
        if (slaveKey != -1) {
            return slaveKey;
        }

        if (masterKey == SymbolTable.VALUE_IS_NULL) {
            if (slaveSymbolTable.containsNullValue()) {
                slaveKey = SymbolTable.VALUE_IS_NULL;
                // add to cache unconditionally even when at the max size, null is important to cache
                masterKeyToSlaveKey.put(masterKey, slaveKey);
                return slaveKey;
            }
            return StaticSymbolTable.VALUE_NOT_FOUND;
        }

        CharSequence strSym = masterRecord.getSymA(masterSymbolIndex);
        slaveKey = slaveSymbolTable.keyOf(strSym);
        if (slaveKey == StaticSymbolTable.VALUE_NOT_FOUND) {
            // We could consider adding a cache also for keys known to be not found.
            // Not implemented for now.
            return slaveKey;
        }

        // we reserve space in the cache for null, so < instead of <=
        if (masterKeyToSlaveKey.size() < maxCacheSize) {
            masterKeyToSlaveKey.put(masterKey, slaveKey);
        }
        return slaveKey;
    }

    @Override
    public boolean isShortCircuit(Record masterRecord) {
        return getSlaveKey(masterRecord) == StaticSymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public void of(TimeFrameRecordCursor slaveCursor) {
        this.slaveSymbolTable = slaveCursor.getSymbolTable(slaveSymbolIndex);
        this.masterKeyToSlaveKey.clear();
        this.maxCacheSize = config.getSqlAsOfJoinShortCircuitCacheCapacity();
    }

    @Override
    public void of(RecordCursor slaveCursor) {
        this.slaveSymbolTable = (StaticSymbolTable) slaveCursor.getSymbolTable(slaveSymbolIndex);
        this.masterKeyToSlaveKey.clear();
        this.maxCacheSize = config.getSqlAsOfJoinShortCircuitCacheCapacity();
    }
}
