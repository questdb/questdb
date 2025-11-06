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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;

public final class StringToSymbolJoinKeyMapping implements SymbolJoinKeyMapping, SymbolShortCircuit {
    private final int masterStringIndex;
    private final int slaveSymbolIndex;
    private StaticSymbolTable slaveSymbolTable;

    public StringToSymbolJoinKeyMapping(int masterStringIndex, int slaveSymbolIndex) {
        this.masterStringIndex = masterStringIndex;
        this.slaveSymbolIndex = slaveSymbolIndex;
    }

    @Override
    public int getSlaveKey(Record masterRecord) {
        CharSequence masterStr = masterRecord.getStrA(masterStringIndex);
        if (masterStr == null) {
            return slaveSymbolTable.containsNullValue()
                    ? StaticSymbolTable.VALUE_IS_NULL
                    : StaticSymbolTable.VALUE_NOT_FOUND;
        }
        return slaveSymbolTable.keyOf(masterStr);
    }

    @Override
    public boolean isShortCircuit(Record masterRecord) {
        return getSlaveKey(masterRecord) == StaticSymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public void of(TimeFrameRecordCursor slaveCursor) {
        this.slaveSymbolTable = slaveCursor.getSymbolTable(slaveSymbolIndex);
    }
}
