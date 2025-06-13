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
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.std.CompactIntHashSet;

public final class SingleSymbolSymbolShortCircuit implements SymbolShortCircuit {
    private final CairoConfiguration config;
    private final CompactIntHashSet masterKeysExistingInSlaveCache = new CompactIntHashSet(16, 0.4);
    private final int masterSymbolIndex;
    private final int slaveSymbolIndex;
    private int maxCacheSize = 0;
    private StaticSymbolTable slaveSymbolTable;

    public SingleSymbolSymbolShortCircuit(CairoConfiguration config, int masterSymbolIndex, int slaveSymbolIndex) {
        this.masterSymbolIndex = masterSymbolIndex;
        this.slaveSymbolIndex = slaveSymbolIndex;
        this.config = config;
    }

    @Override
    public boolean isShortCircuit(Record masterRecord) {
        assert slaveSymbolTable != null : "slaveSymbolTable must be set before calling isShortCircuit";

        int masterKey = masterRecord.getInt(masterSymbolIndex);
        if (!masterKeysExistingInSlaveCache.excludes(masterKey)) {
            return false;
        }

        if (masterKey == SymbolTable.VALUE_IS_NULL) {
            if (slaveSymbolTable.containsNullValue()) {
                // add to cache unconditionally even when at the max size, null is important to cache
                masterKeysExistingInSlaveCache.add(masterKey);
                return false;
            }
            return true;
        }

        CharSequence strSym = masterRecord.getSymA(masterSymbolIndex);
        if (slaveSymbolTable.keyOf(strSym) == StaticSymbolTable.VALUE_NOT_FOUND) {
            // we could consider adding a cache also for keys known to be not found
            // not implemented for now
            return true;
        }

        // we reserve space in the cache for null, so < instead of <=
        if (masterKeysExistingInSlaveCache.size() < maxCacheSize) {
            masterKeysExistingInSlaveCache.add(masterKey);
        }
        return false;
    }

    @Override
    public void of(TimeFrameRecordCursor slaveCursor) {
        this.slaveSymbolTable = slaveCursor.getSymbolTable(slaveSymbolIndex);
        this.masterKeysExistingInSlaveCache.clear();
        this.maxCacheSize = config.getSqlAsOfJoinShortCircuitCacheCapacity();
    }
}
