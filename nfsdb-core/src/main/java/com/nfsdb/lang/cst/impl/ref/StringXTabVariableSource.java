/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.lang.cst.impl.ref;

import com.nfsdb.lang.cst.*;
import com.nfsdb.storage.SymbolTable;

/**
 * Reads partition/rowid from current position of JoinedSource, reads symbol column value based on current position of said JoinedSource
 */
public class StringXTabVariableSource implements IntVariableSource, IntVariable {
    private final RecordSourceState state;
    private final String slaveSymbol;
    private final int masterColumnIndex;
    private SymbolTable slaveTab;
    private int slaveKey;

    public StringXTabVariableSource(RecordMetadata masterMetadata, String masterSymbol, String slaveSymbol, RecordSourceState state) {
        this.slaveSymbol = slaveSymbol;
        this.masterColumnIndex = masterMetadata.getColumnIndex(masterSymbol);
        this.state = state;
    }

    @Override
    public int getValue() {
        if (slaveKey == -3) {
            slaveKey = slaveTab.getQuick(state.currentRecord().getStr(masterColumnIndex).toString());
        }
        return slaveKey;
    }

    @Override
    public IntVariable getVariable(PartitionSlice slice) {
        if (slaveTab == null) {
            slaveTab = slice.partition.getJournal().getSymbolTable(slaveSymbol);
        }
        return this;
    }

    @Override
    public void reset() {
        slaveKey = -3;
    }
}
