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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.ObjList;

/**
 * Symbol table source for multi-slave HORIZON JOIN queries that routes symbol table
 * lookups to the correct source (master or one of N slaves) based on column mappings.
 */
class MultiHorizonJoinSymbolTableSource implements SymbolTableSource {
    private final int[] columnIndices;
    private final int[] columnSources;
    private final ObjList<SymbolTableSource> slaveSources;
    private SymbolTableSource masterSource;

    MultiHorizonJoinSymbolTableSource(int[] columnSources, int[] columnIndices, int slaveCount) {
        this.columnSources = columnSources;
        this.columnIndices = columnIndices;
        this.slaveSources = new ObjList<>(slaveCount);
        this.slaveSources.setPos(slaveCount);
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        int source = columnSources[columnIndex];
        int sourceColumnIndex = columnIndices[columnIndex];
        if (source == MultiHorizonJoinRecord.SOURCE_MASTER) {
            return masterSource.getSymbolTable(sourceColumnIndex);
        }
        if (source >= MultiHorizonJoinRecord.SOURCE_SLAVE_BASE) {
            SymbolTableSource slaveSource = slaveSources.getQuick(source - MultiHorizonJoinRecord.SOURCE_SLAVE_BASE);
            return slaveSource != null ? slaveSource.getSymbolTable(sourceColumnIndex) : null;
        }
        return null;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        int source = columnSources[columnIndex];
        int sourceColumnIndex = columnIndices[columnIndex];
        if (source == MultiHorizonJoinRecord.SOURCE_MASTER) {
            return masterSource.newSymbolTable(sourceColumnIndex);
        }
        if (source >= MultiHorizonJoinRecord.SOURCE_SLAVE_BASE) {
            SymbolTableSource slaveSource = slaveSources.getQuick(source - MultiHorizonJoinRecord.SOURCE_SLAVE_BASE);
            return slaveSource != null ? slaveSource.newSymbolTable(sourceColumnIndex) : null;
        }
        return null;
    }

    void of(SymbolTableSource masterSource, ObjList<SymbolTableSource> slaveSources) {
        this.masterSource = masterSource;
        for (int i = 0, n = this.slaveSources.size(); i < n; i++) {
            this.slaveSources.setQuick(i, slaveSources.getQuick(i));
        }
    }
}
